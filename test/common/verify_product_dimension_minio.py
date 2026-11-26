"""Verify G5 Product Dimension using isolated MinIO input and output buckets."""

import argparse
import json
import tempfile
from hashlib import sha256
from pathlib import Path

import pyarrow.parquet as pq

from loader.common.minio_store import MinioLoader
from pipeline import build_product_dimension, create_loader


SOURCE_BUCKET = "nutee-sequential-integrity-g4-test-20261126"
BASELINES = {
    "rees46_electronics": {
        "version": "cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7",
        "adapter": "rees46_events",
        "silver_rows": 885129,
        "gold_products": 53453,
    },
    "merrec": {
        "version": "3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca",
        "adapter": "merrec_events",
        "silver_rows": 605269,
        "gold_products": 416784,
    },
}
BASELINE_BUCKETS = {
    "nutee-session-v2-test-20261126b",
    "nutee-event-preservation-g3-test-20261126",
    SOURCE_BUCKET,
}
REPORT_PATH = Path(__file__).resolve().parents[2] / "reports" / "product_dimension_g5" / "integration.json"


def _snapshot(client, bucket: str) -> dict:
    return {
        item.object_name: {
            "etag": item.etag,
            "size": item.size,
            "last_modified": item.last_modified.isoformat() if item.last_modified else None,
            "version_id": item.version_id,
        }
        for item in client.list_objects(bucket, recursive=True)
    }


def _snapshot_summary(client, bucket: str) -> dict:
    snapshot = _snapshot(client, bucket)
    digest = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {"object_count": len(snapshot), "sha256": sha256(digest).hexdigest()}


def _product_ids(client, bucket: str, key: str, column: str, temporary: Path) -> tuple[set[str], int]:
    path = temporary / f"{column}-{sha256(key.encode('utf-8')).hexdigest()[:12]}.parquet"
    client.fget_object(bucket, key, str(path))
    with pq.ParquetFile(path) as parquet:
        rows = parquet.metadata.num_rows
        product_ids = set()
        for batch in parquet.iter_batches(columns=[column]):
            for value in batch.column(0).to_pylist():
                if not isinstance(value, str) or not value.strip():
                    raise AssertionError(f"{key} contains a null or empty {column}")
                product_ids.add(value)
    return product_ids, rows


def _process_dataset(client, source_store, test_store, dataset: str, expected: dict, temporary: Path) -> dict:
    version = expected["version"]
    silver_key = f"silver/{dataset}/{expected['adapter']}/{version}/events.parquet"
    gold_key = f"gold/v3/{dataset}/{version}/events.parquet"
    products_key = f"gold/v3/{dataset}/{version}/products.parquet"

    silver_ids, silver_rows = _product_ids(client, SOURCE_BUCKET, silver_key, "product_id", temporary)
    gold_ids, gold_rows = _product_ids(client, SOURCE_BUCKET, gold_key, "product_id", temporary)
    first_key, first_stats = build_product_dimension(
        test_store,
        {"name": dataset, "adapter": expected["adapter"]},
        version,
        source_loader=source_store,
    )
    second_key, second_stats = build_product_dimension(
        test_store,
        {"name": dataset, "adapter": expected["adapter"]},
        version,
        source_loader=source_store,
    )
    dimension_ids, dimension_rows = _product_ids(
        client, test_store.bucket, products_key, "product_id", temporary
    )
    if first_key != products_key or second_key != products_key:
        raise AssertionError("Product Dimension object key differs from contract")

    coverage_count = len(gold_ids.intersection(dimension_ids))
    criteria = {
        "silver_rows_preserved": {
            "expected": expected["silver_rows"],
            "actual": silver_rows,
            "status": "PASS" if silver_rows == expected["silver_rows"] else "FAIL",
        },
        "distinct_silver_product_count": {
            "expected": len(silver_ids),
            "actual": dimension_rows,
            "status": "PASS" if dimension_rows == len(silver_ids) else "FAIL",
        },
        "silver_product_id_set_matches": {
            "expected": len(silver_ids),
            "actual": len(silver_ids.intersection(dimension_ids)),
            "status": "PASS" if silver_ids == dimension_ids else "FAIL",
        },
        "product_ids_nonnull_unique": {
            "expected": dimension_rows,
            "actual": len(dimension_ids),
            "status": "PASS" if len(dimension_ids) == dimension_rows else "FAIL",
        },
        "gold_v3_distinct_product_baseline": {
            "expected": expected["gold_products"],
            "actual": len(gold_ids),
            "status": "PASS" if len(gold_ids) == expected["gold_products"] else "FAIL",
        },
        "gold_v3_product_coverage": {
            "expected": len(gold_ids),
            "actual": coverage_count,
            "status": "PASS" if coverage_count == len(gold_ids) else "FAIL",
        },
        "metadata_traceable_to_silver": {
            "expected": 0,
            "actual": sum(first_stats["untraceable_metadata_values"].values()),
            "status": "PASS" if not any(first_stats["untraceable_metadata_values"].values()) else "FAIL",
        },
        "deterministic_rerun": {
            "expected": first_stats["content_sha256"],
            "actual": second_stats["content_sha256"],
            "status": "PASS" if first_stats == second_stats else "FAIL",
        },
    }
    return {
        "silver_key": silver_key,
        "gold_v3_key": gold_key,
        "product_dimension_key": products_key,
        "silver_rows": silver_rows,
        "silver_distinct_products": len(silver_ids),
        "gold_v3_rows": gold_rows,
        "gold_v3_distinct_products": len(gold_ids),
        "gold_v3_coverage_count": coverage_count,
        "gold_v3_coverage_rate": coverage_count / len(gold_ids) if gold_ids else 1.0,
        "product_dimension_rows": dimension_rows,
        "product_dimension_distinct_products": len(dimension_ids),
        "missing_counts": first_stats["missing_counts"],
        "missing_rates": first_stats["missing_rates"],
        "metadata_conflicts": first_stats["metadata_conflicts"],
        "products_with_metadata_conflicts": first_stats["products_with_metadata_conflicts"],
        "metadata_traceability_checks": first_stats["untraceable_metadata_values"],
        "content_sha256": first_stats["content_sha256"],
        "rerun_content_sha256": second_stats["content_sha256"],
        "acceptance_criteria": criteria,
    }


def run(test_bucket: str) -> dict:
    official = create_loader()
    if not official.client.bucket_exists(official.bucket):
        raise RuntimeError("Official MinIO bucket is unavailable; refusing to run")
    if not official.client.bucket_exists(SOURCE_BUCKET):
        raise RuntimeError(f"Accepted Silver/Gold V3 bucket {SOURCE_BUCKET} is unavailable")
    if test_bucket == official.bucket or test_bucket in BASELINE_BUCKETS:
        raise ValueError("G5 output must use a new bucket, separate from official and accepted baselines")
    if official.client.bucket_exists(test_bucket):
        raise ValueError("G5 requires a new test bucket; refusing to overwrite existing objects")

    versioned_buckets = {official.bucket, SOURCE_BUCKET, *BASELINE_BUCKETS}
    versioning_before = {
        bucket: official.client.get_bucket_versioning(bucket).status
        for bucket in sorted(versioned_buckets)
        if official.client.bucket_exists(bucket)
    }
    inventories_before = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted(versioned_buckets)
        if official.client.bucket_exists(bucket)
    }
    for dataset, expected in BASELINES.items():
        version = expected["version"]
        for key in (
            f"silver/{dataset}/{expected['adapter']}/{version}/events.parquet",
            f"gold/v3/{dataset}/{version}/events.parquet",
        ):
            official.client.stat_object(SOURCE_BUCKET, key)

    source_store = MinioLoader(official.client, SOURCE_BUCKET)
    test_store = MinioLoader(official.client, test_bucket)
    test_store.ensure_bucket()
    with tempfile.TemporaryDirectory(prefix="nutee-g5-products-") as directory:
        temporary = Path(directory)
        results = {
            dataset: _process_dataset(
                official.client, source_store, test_store, dataset, expected, temporary
            )
            for dataset, expected in BASELINES.items()
        }

    inventories_after = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted(versioned_buckets)
        if official.client.bucket_exists(bucket)
    }
    versioning_after = {
        bucket: official.client.get_bucket_versioning(bucket).status
        for bucket in sorted(versioned_buckets)
        if official.client.bucket_exists(bucket)
    }
    baseline_unchanged = inventories_before == inventories_after
    versioning_unchanged = versioning_before == versioning_after
    all_criteria_pass = all(
        criterion["status"] == "PASS"
        for result in results.values()
        for criterion in result["acceptance_criteria"].values()
    )
    report = {
        "status": "PASS" if baseline_unchanged and versioning_unchanged and all_criteria_pass else "FAIL",
        "official_bucket": official.bucket,
        "source_bucket": SOURCE_BUCKET,
        "test_bucket": test_bucket,
        "baseline_inventories_before": inventories_before,
        "baseline_inventories_after": inventories_after,
        "baseline_objects_unchanged": baseline_unchanged,
        "bucket_versioning_before": versioning_before,
        "bucket_versioning_after": versioning_after,
        "bucket_versioning_unchanged": versioning_unchanged,
        "test_objects_retained": True,
        "datasets": results,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if report["status"] != "PASS":
        raise AssertionError("One or more G5 acceptance criteria failed; see integration report")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-bucket", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.test_bucket), indent=2))


if __name__ == "__main__":
    main()
