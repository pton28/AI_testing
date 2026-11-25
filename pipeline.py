import argparse
import json
import os
import re
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory

from dotenv import load_dotenv
from minio import Minio

from extractor.common.source import extract
from extractor.common.minio_source import extract_object
from loader.common.minio_store import MinioLoader
from loader.common.local_store import LocalLoader
from transformer.common.silver import write_silver
from transformer.common.gold import validate_gold, write_gold
from transformer.registry import ADAPTERS, GOLD_ADAPTERS, SILVER_ASSET_ADAPTERS


def create_loader(storage: str = "minio") -> MinioLoader | LocalLoader:
    project_dir = Path(__file__).resolve().parent
    if storage == "local":
        return LocalLoader(project_dir / "data")
    if storage != "minio":
        raise ValueError(f"Unknown storage {storage!r}")
    load_dotenv(project_dir / ".env")
    return MinioLoader(
        Minio(
            os.getenv("MINIO_ENDPOINT_URL", "localhost:9000"),
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        ),
        os.getenv("MINIO_BUCKET", "nutee-recsys"),
    )


def stored_file(loader: MinioLoader | LocalLoader, key: str):
    """Select a local file or the existing MinIO extractor for transformation."""
    if isinstance(loader, LocalLoader):
        return nullcontext(loader.path(key))
    return extract_object(loader.client, loader.bucket, key)


def build_gold(loader: MinioLoader | LocalLoader, dataset: dict, source_version: str) -> tuple[str, dict]:
    name = dataset["name"]
    adapter_name = dataset["adapter"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name):
        raise ValueError(f"Invalid dataset name {name!r}")
    if not re.fullmatch(r"[a-f0-9]{64}", source_version):
        raise ValueError("source_version must be the SHA-256 used in the Silver key")
    adapter = GOLD_ADAPTERS[adapter_name]
    silver_key = f"silver/{name}/{adapter_name}/{source_version}/events.parquet"
    gold_key = f"gold/{name}/{source_version}/events.parquet"
    with stored_file(loader, silver_key) as silver_file, TemporaryDirectory(prefix="nutee-gold-") as directory:
        gold_file = Path(directory) / "events.parquet"
        stats = write_gold(silver_file, gold_file, adapter, name, source_version)
        loader.load(gold_key, gold_file, "application/vnd.apache.parquet")
    with stored_file(loader, gold_key) as stored_gold:
        if validate_gold(stored_gold, name) != stats:
            raise ValueError("Gold readback does not match generated events")
    print(f"Gold: {loader.location(gold_key)} | {json.dumps(stats)}", flush=True)
    return gold_key, stats


def run() -> None:
    parser = argparse.ArgumentParser(description="NUTEE Bronze, Silver and Gold pipeline")
    parser.add_argument("--storage", choices=("minio", "local"), default="minio", help="Output storage (default: minio)")
    parser.add_argument("--silver-only", action="store_true", help="Normalize configured session/catalog assets without assigning Gold event types")
    parser.add_argument("--gold-only", action="store_true", help="Build Gold from existing Silver in the selected storage")
    parser.add_argument("--source-version", help="Source SHA-256 in the Silver key (required with --gold-only)")
    parser.add_argument("--dataset", help="Only process this configured dataset")
    args = parser.parse_args()
    if args.silver_only and (args.gold_only or args.source_version or not args.dataset):
        parser.error("--silver-only requires --dataset and cannot be combined with --gold-only/--source-version")
    if args.gold_only != bool(args.source_version):
        parser.error("--gold-only and --source-version must be used together")
    project_dir = Path(__file__).resolve().parent
    config_path = project_dir / "data" / "datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    candidates = config["datasets"] + (config.get("planned_datasets", []) if args.silver_only else [])
    datasets = [item for item in candidates if args.dataset is None or item["name"] == args.dataset]
    if not datasets:
        parser.error("No matching configured dataset")
    if args.silver_only:
        for dataset in datasets:
            if not dataset.get("files") or any(asset.get("adapter") not in SILVER_ASSET_ADAPTERS for asset in dataset["files"]):
                parser.error("This dataset has no registered Silver asset adapters")
    loader = create_loader(args.storage)
    if args.storage == "minio":
        loader.ensure_bucket()

    for dataset in datasets:
        name = dataset["name"]
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name):
            raise ValueError(f"Invalid dataset name {name!r}")
        if args.silver_only:
            for asset in dataset["files"]:
                adapter_name = asset["adapter"]
                with extract(asset["source"], config_path.parent) as (source, checksum):
                    loader.load(f"bronze/{name}/{checksum}/{source.name}", source, "application/octet-stream")
                    silver_file = source.with_name("normalized.parquet")
                    rows = write_silver(source, silver_file, SILVER_ASSET_ADAPTERS[adapter_name], source_version=checksum)
                    key = f"silver/{name}/{adapter_name}/{checksum}/data.parquet"
                    loader.load(key, silver_file, "application/vnd.apache.parquet")
                    print(f"Silver: {loader.location(key)} | {rows:,} rows", flush=True)
            continue
        adapter_name = dataset["adapter"]
        adapter = ADAPTERS[adapter_name]
        if args.gold_only:
            build_gold(loader, dataset, args.source_version)
            continue
        with extract(dataset["source"], config_path.parent) as (source, checksum):
            bronze_key = f"bronze/{name}/{checksum}/{source.name}"
            silver_key = f"silver/{name}/{adapter_name}/{checksum}/events.parquet"
            loader.load(bronze_key, source, "application/octet-stream")
            print(f"Bronze: {loader.location(bronze_key)}", flush=True)

            silver_file = source.with_name("normalized-events.parquet")
            rows = write_silver(source, silver_file, adapter)
            loader.load(silver_key, silver_file, "application/vnd.apache.parquet")
            print(f"Silver: {loader.location(silver_key)} | {rows:,} rows", flush=True)
        build_gold(loader, dataset, checksum)
