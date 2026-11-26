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
from transformer.common.gold import GOLD_SCHEMA, GOLD_SCHEMA_V3, validate_gold, write_gold
from transformer.common.product_dimension import (
    validate_product_dimension,
    write_product_dimension,
)
from transformer.common.model_ready import (
    validate_als_interactions,
    validate_session_sequences,
    write_model_ready,
)
from transformer.registry import ADAPTERS, GOLD_ADAPTERS, PRODUCT_NAME_COLUMNS, SILVER_ASSET_ADAPTERS


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


def build_gold(
    loader: MinioLoader | LocalLoader,
    dataset: dict,
    source_version: str,
    schema=GOLD_SCHEMA,
) -> tuple[str, dict]:
    name = dataset["name"]
    adapter_name = dataset["adapter"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name):
        raise ValueError(f"Invalid dataset name {name!r}")
    if not re.fullmatch(r"[a-f0-9]{64}", source_version):
        raise ValueError("source_version must be the SHA-256 used in the Silver key")
    if schema.equals(GOLD_SCHEMA_V3, check_metadata=False):
        gold_version = "v3"
    elif schema.equals(GOLD_SCHEMA, check_metadata=False):
        gold_version = "v2"
    else:
        raise ValueError("Gold pipeline only supports V2 and V3 output")
    adapter = GOLD_ADAPTERS[adapter_name]
    silver_key = f"silver/{name}/{adapter_name}/{source_version}/events.parquet"
    gold_key = f"gold/{gold_version}/{name}/{source_version}/events.parquet"
    with stored_file(loader, silver_key) as silver_file, TemporaryDirectory(prefix="nutee-gold-") as directory:
        gold_file = Path(directory) / "events.parquet"
        stats = write_gold(silver_file, gold_file, adapter, name, source_version, schema=schema)
        loader.load(gold_key, gold_file, "application/vnd.apache.parquet")
    with stored_file(loader, gold_key) as stored_gold:
        if validate_gold(stored_gold, name, schema=schema) != stats:
            raise ValueError("Gold readback does not match generated events")
    print(f"Gold: {loader.location(gold_key)} | {json.dumps(stats)}", flush=True)
    return gold_key, stats


def build_product_dimension(
    loader: MinioLoader | LocalLoader,
    dataset: dict,
    source_version: str,
    *,
    source_loader: MinioLoader | LocalLoader | None = None,
) -> tuple[str, dict]:
    name = dataset["name"]
    adapter_name = dataset["adapter"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name):
        raise ValueError(f"Invalid dataset name {name!r}")
    if not re.fullmatch(r"[a-f0-9]{64}", source_version):
        raise ValueError("source_version must be the SHA-256 used in the Silver key")
    if adapter_name not in PRODUCT_NAME_COLUMNS:
        raise ValueError(f"No Product Dimension column mapping for adapter {adapter_name!r}")
    product_name_column = PRODUCT_NAME_COLUMNS[adapter_name]
    input_loader = source_loader or loader
    silver_key = f"silver/{name}/{adapter_name}/{source_version}/events.parquet"
    products_key = f"gold/v3/{name}/{source_version}/products.parquet"
    with stored_file(input_loader, silver_key) as silver_file, TemporaryDirectory(prefix="nutee-products-") as directory:
        products_file = Path(directory) / "products.parquet"
        stats = write_product_dimension(
            silver_file,
            products_file,
            name,
            product_name_column=product_name_column,
        )
        loader.load(products_key, products_file, "application/vnd.apache.parquet")
    with stored_file(loader, products_key) as stored_products:
        readback = validate_product_dimension(stored_products, name)
    for field in ("product_count", "missing_counts", "missing_rates", "content_sha256"):
        if readback[field] != stats[field]:
            raise ValueError("Product Dimension readback does not match generated output")
    print(f"Product Dimension: {loader.location(products_key)} | {json.dumps(stats)}", flush=True)
    return products_key, stats


def build_model_ready(
    loader: MinioLoader | LocalLoader,
    dataset: dict,
    source_version: str,
    *,
    source_loader: MinioLoader | LocalLoader | None = None,
    product_loader: MinioLoader | LocalLoader | None = None,
) -> tuple[dict, dict]:
    name = dataset["name"]
    adapter_name = dataset["adapter"]
    if name not in {"rees46_electronics", "merrec"}:
        raise ValueError(f"Model-ready outputs are not configured for {name!r}")
    if not re.fullmatch(r"[a-f0-9]{64}", source_version):
        raise ValueError("source_version must be the SHA-256 used in the Gold V3 key")
    gold_input = source_loader or loader
    products_input = product_loader or gold_input
    gold_key = f"gold/v3/{name}/{source_version}/events.parquet"
    products_key = f"gold/v3/{name}/{source_version}/products.parquet"
    als_key = f"gold/v3/{name}/{source_version}/model_ready/als_interactions.parquet"
    sequences_key = f"gold/v3/{name}/{source_version}/model_ready/session_sequences.parquet"
    with (
        stored_file(gold_input, gold_key) as gold_file,
        stored_file(products_input, products_key) as products_file,
        TemporaryDirectory(prefix="nutee-model-ready-output-") as directory,
    ):
        als_file = Path(directory) / "als_interactions.parquet"
        sequences_file = Path(directory) / "session_sequences.parquet"
        stats = write_model_ready(
            gold_file,
            products_file,
            als_file,
            sequences_file,
            name,
        )
        loader.load(als_key, als_file, "application/vnd.apache.parquet")
        loader.load(sequences_key, sequences_file, "application/vnd.apache.parquet")
    with stored_file(loader, als_key) as stored_als:
        als_readback = validate_als_interactions(stored_als)
    with stored_file(loader, sequences_key) as stored_sequences:
        sequence_readback = validate_session_sequences(stored_sequences)
    for field in ("pair_count", "interaction_count", "content_sha256"):
        if als_readback[field] != stats["als_interactions"][field]:
            raise ValueError("ALS interaction readback does not match generated output")
    for field in ("sequence_count", "sequence_event_count", "ambiguous_session_count", "content_sha256"):
        if sequence_readback[field] != stats["session_sequences"][field]:
            raise ValueError("Session sequence readback does not match generated output")
    keys = {"als_interactions": als_key, "session_sequences": sequences_key}
    print(f"Model-ready: {json.dumps({'keys': keys, **stats})}", flush=True)
    return keys, stats


def run() -> None:
    parser = argparse.ArgumentParser(description="NUTEE Bronze, Silver, Gold and model-ready pipeline")
    parser.add_argument("--storage", choices=("minio", "local"), default="minio", help="Output storage (default: minio)")
    parser.add_argument("--silver-only", action="store_true", help="Normalize configured session/catalog assets without assigning Gold event types")
    parser.add_argument("--gold-only", action="store_true", help="Build Gold from existing Silver in the selected storage")
    parser.add_argument("--product-dimension-only", action="store_true", help="Build Product Dimension from existing Silver")
    parser.add_argument("--model-ready-only", action="store_true", help="Build model-ready aggregates from existing Gold V3")
    parser.add_argument(
        "--source-version",
        help="Source SHA-256 in the Gold/Silver key (required with --gold-only, --product-dimension-only or --model-ready-only)",
    )
    parser.add_argument("--dataset", help="Only process this configured dataset")
    args = parser.parse_args()
    if args.silver_only and (
        args.gold_only or args.product_dimension_only or args.model_ready_only
        or args.source_version or not args.dataset
    ):
        parser.error(
            "--silver-only requires --dataset and cannot be combined with "
            "--gold-only/--product-dimension-only/--model-ready-only/--source-version"
        )
    if sum((args.gold_only, args.product_dimension_only, args.model_ready_only)) > 1:
        parser.error("--gold-only, --product-dimension-only and --model-ready-only are mutually exclusive")
    source_version_mode = args.gold_only or args.product_dimension_only or args.model_ready_only
    if source_version_mode != bool(args.source_version):
        parser.error(
            "A source-version-only mode and --source-version must be used together"
        )
    if (args.product_dimension_only or args.model_ready_only) and not args.dataset:
        parser.error("--product-dimension-only and --model-ready-only require --dataset")
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
        if args.product_dimension_only:
            build_product_dimension(loader, dataset, args.source_version)
            continue
        if args.model_ready_only:
            build_model_ready(loader, dataset, args.source_version)
            continue
        adapter_name = dataset["adapter"]
        adapter = ADAPTERS[adapter_name]
        gold_schema = (
            GOLD_SCHEMA_V3
            if name in {"rees46_electronics", "merrec"}
            else GOLD_SCHEMA
        )
        if args.gold_only:
            build_gold(loader, dataset, args.source_version, schema=gold_schema)
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
        build_gold(loader, dataset, checksum, schema=gold_schema)
