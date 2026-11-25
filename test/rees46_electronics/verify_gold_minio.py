"""Explicit integration acceptance: build Gold twice and inspect real MinIO."""

import argparse
import json
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

from extractor.common.minio_source import extract_object
from pipeline import build_gold, create_loader
from transformer.common.silver import BATCH_SIZE
from transformer.common.gold import validate_gold


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-version", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    config = json.loads((root / "data" / "datasets.json").read_text(encoding="utf-8-sig"))
    dataset = next(item for item in config["datasets"] if item["name"] == "rees46_electronics")
    store = create_loader()
    silver_key = f"silver/{dataset['name']}/{dataset['adapter']}/{args.source_version}/events.parquet"
    with extract_object(store.client, store.bucket, silver_key) as path:
        with pq.ParquetFile(path) as silver:
            silver_rows = silver.metadata.num_rows
            counts = Counter()
            null_sessions = 0
            for batch in silver.iter_batches(batch_size=BATCH_SIZE, columns=["event_type", "session_id"]):
                counts.update(batch.column(0).to_pylist())
                null_sessions += batch.column(1).null_count
    key, first = build_gold(store, dataset, args.source_version)
    first_etag = store.client.stat_object(store.bucket, key).etag
    second_key, second = build_gold(store, dataset, args.source_version)
    second_etag = store.client.stat_object(store.bucket, key).etag
    with extract_object(store.client, store.bucket, key) as path:
        readback = validate_gold(path, dataset["name"])
    if key != second_key or first != second or second != readback or first_etag != second_etag:
        raise AssertionError("Gold changed on identical-input rerun")
    if first["rows"] != silver_rows or first["event_counts"] != dict(counts):
        raise AssertionError("Gold rows/distribution differ from Silver")
    if first["null_counts"]["session_id"] != null_sessions:
        raise AssertionError("Gold changed null session values")
    for name in ("position_in_session", "query_text", "rating_value"):
        if first["null_counts"][name] != silver_rows:
            raise AssertionError(f"REES46 {name} must remain null")
    objects = list(store.client.list_objects(store.bucket, prefix=key, recursive=True))
    versions = list(store.client.list_objects(store.bucket, prefix=key, recursive=True, include_version=True))
    if len(objects) != 1 or objects[0].object_name != key:
        raise AssertionError("Unexpected Gold objects")
    expected = {"product_view": 793748, "add_to_cart": 54035, "purchase": 37346}
    report = {"silver_key": silver_key, "gold_uri": f"s3://{store.bucket}/{key}",
              "source_version": args.source_version, "silver_rows": silver_rows,
              "silver_event_counts": dict(counts), "gold": first,
              "matches_reference_counts": dict(counts) == expected,
              "rerun_equal": True, "etag": second_etag, "gold_object_count": len(objects),
              "gold_object_versions": len(versions),
              "bucket_versioning": store.client.get_bucket_versioning(store.bucket).status}
    output = root / "outputs" / dataset["name"] / "gold_acceptance.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
