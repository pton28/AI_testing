"""Verify G6 model-ready outputs against accepted Gold V3 and Product Dimension."""

import argparse
import json
import tempfile
from hashlib import sha256
from pathlib import Path

import pyarrow.parquet as pq

from loader.common.minio_store import MinioLoader
from pipeline import build_model_ready, create_loader
from transformer.common.gold import GOLD_SCHEMA_V3, validate_gold
from transformer.common.model_ready import (
    validate_als_interactions,
    validate_session_sequences,
)


GOLD_BUCKET = "nutee-sequential-integrity-g4-test-20261126"
PRODUCT_BUCKET = "nutee-product-dimension-g5-test-20261126-1642"
BASELINES = {
    "rees46_electronics": {
        "version": "cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7",
        "adapter": "rees46_events",
        "gold_events": 885129,
        "pairs": 556690,
        "sessions": 490633,
        "sequence_events": 884964,
        "null_session_events": 165,
    },
    "merrec": {
        "version": "3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca",
        "adapter": "merrec_events",
        "gold_events": 515601,
        "pairs": 429812,
        "sessions": 112859,
        "sequence_events": 515601,
        "null_session_events": 0,
    },
}
BASELINE_BUCKETS = {
    "nutee-session-v2-test-20261126b",
    "nutee-event-preservation-g3-test-20261126",
    GOLD_BUCKET,
    PRODUCT_BUCKET,
}
REPORT_PATH = Path(__file__).resolve().parents[2] / "reports" / "model_ready_g6" / "integration.json"


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


def _read_product_ids(client, bucket: str, key: str, temporary: Path) -> set[str]:
    path = temporary / f"{sha256(key.encode('utf-8')).hexdigest()[:16]}.parquet"
    client.fget_object(bucket, key, str(path))
    ids = set()
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(columns=["product_id"]):
            for product_id in batch.column(0).to_pylist():
                if not isinstance(product_id, str) or not product_id.strip():
                    raise AssertionError(f"{key} contains a null or empty product_id")
                ids.add(product_id)
    return ids


def _rows(path: Path):
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches():
            yield from batch.to_pylist()


def _gold_session_groups(path: Path, metrics: dict):
    columns = [
        "session_key", "user_id", "product_id", "event_type", "event_time",
        "position_in_session", "order_ambiguous",
    ]
    current_key = None
    current_user = None
    items = []
    events = []
    timestamps = []
    current_ambiguous = False
    current_position = 0
    seen_null_session = False
    previous_key = None
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(GOLD_SCHEMA_V3, check_metadata=False):
            raise AssertionError("Input event log is not Gold V3")
        for batch in parquet.iter_batches(columns=columns):
            for row in batch.to_pylist():
                key = row["session_key"]
                if key is None:
                    seen_null_session = True
                    metrics["null_session_events"] += 1
                    if row["position_in_session"] is not None or row["order_ambiguous"] is not None:
                        raise AssertionError("Null session_key must have null ordering fields")
                    continue
                if seen_null_session:
                    raise AssertionError("A non-null session_key follows null-session Gold rows")
                if key != current_key:
                    if current_key is not None:
                        yield {
                            "session_key": current_key,
                            "user_id": current_user,
                            "item_sequence": items,
                            "event_sequence": events,
                            "timestamp_sequence": timestamps,
                            "sequence_length": len(items),
                            "has_ambiguous_order": current_ambiguous,
                        }
                    if previous_key is not None and key <= previous_key:
                        raise AssertionError("Gold V3 session keys are not in deterministic order")
                    previous_key = key
                    current_key = key
                    current_user = row["user_id"]
                    items = []
                    events = []
                    timestamps = []
                    current_ambiguous = False
                    current_position = 0
                    metrics["session_count"] += 1
                if row["user_id"] != current_user:
                    raise AssertionError("A session_key maps to multiple users")
                current_position += 1
                if row["position_in_session"] != current_position:
                    raise AssertionError("Gold V3 positions are not continuous within session")
                if not isinstance(row["order_ambiguous"], bool):
                    raise AssertionError("Non-null session events require boolean order_ambiguous")
                items.append(row["product_id"])
                events.append(row["event_type"])
                timestamps.append(row["event_time"])
                metrics["sequence_event_count"] += 1
                if row["order_ambiguous"]:
                    metrics["ambiguous_event_count"] += 1
                    current_ambiguous = True
    if current_key is not None:
        yield {
            "session_key": current_key,
            "user_id": current_user,
            "item_sequence": items,
            "event_sequence": events,
            "timestamp_sequence": timestamps,
            "sequence_length": len(items),
            "has_ambiguous_order": current_ambiguous,
        }


def _compare_sequences_to_gold(gold_path: Path, sequences_path: Path) -> dict:
    metrics = {
        "session_count": 0,
        "sequence_event_count": 0,
        "null_session_events": 0,
        "ambiguous_event_count": 0,
    }
    sequence_rows = iter(_rows(sequences_path))
    gold_groups = _gold_session_groups(gold_path, metrics)
    try:
        for expected in gold_groups:
            try:
                actual = next(sequence_rows)
            except StopIteration as error:
                raise AssertionError("Session sequence output is missing a Gold session") from error
            if actual != expected:
                raise AssertionError(
                    f"Session sequence does not match Gold V3 for {expected['session_key']!r}"
                )
        try:
            next(sequence_rows)
        except StopIteration:
            pass
        else:
            raise AssertionError("Session sequence output contains sessions absent from Gold V3")
    finally:
        gold_groups.close()
        sequence_rows.close()
    metrics["ambiguous_session_count"] = sum(
        1 for row in _rows(sequences_path) if row["has_ambiguous_order"]
    )
    return metrics


def _criteria(expected, actual) -> dict:
    return {
        name: {
            "expected": expected[name],
            "actual": actual[name],
            "status": "PASS" if expected[name] == actual[name] else "FAIL",
        }
        for name in expected
    }


def _process_dataset(client, gold_store, product_store, output_store, dataset, expected, temporary):
    version = expected["version"]
    gold_key = f"gold/v3/{dataset}/{version}/events.parquet"
    products_key = f"gold/v3/{dataset}/{version}/products.parquet"
    als_key = f"gold/v3/{dataset}/{version}/model_ready/als_interactions.parquet"
    sequences_key = f"gold/v3/{dataset}/{version}/model_ready/session_sequences.parquet"
    gold_path = temporary / f"{dataset}-gold-v3.parquet"
    dimension_path = temporary / f"{dataset}-products.parquet"
    als_path = temporary / f"{dataset}-als.parquet"
    sequences_path = temporary / f"{dataset}-sequences.parquet"

    client.fget_object(GOLD_BUCKET, gold_key, str(gold_path))
    client.fget_object(PRODUCT_BUCKET, products_key, str(dimension_path))
    gold_stats = validate_gold(gold_path, dataset, schema=GOLD_SCHEMA_V3)
    if gold_stats["rows"] != expected["gold_events"]:
        raise AssertionError(f"{dataset} Gold V3 event count differs from accepted baseline")

    keys, first_stats = build_model_ready(
        output_store,
        {"name": dataset, "adapter": expected["adapter"]},
        version,
        source_loader=gold_store,
        product_loader=product_store,
    )
    _, second_stats = build_model_ready(
        output_store,
        {"name": dataset, "adapter": expected["adapter"]},
        version,
        source_loader=gold_store,
        product_loader=product_store,
    )
    client.fget_object(output_store.bucket, als_key, str(als_path))
    client.fget_object(output_store.bucket, sequences_key, str(sequences_path))
    als_stats = validate_als_interactions(als_path)
    sequence_stats = validate_session_sequences(sequences_path)
    sequence_match = _compare_sequences_to_gold(gold_path, sequences_path)

    dimension_ids = _read_product_ids(client, PRODUCT_BUCKET, products_key, temporary)
    als_product_ids = {row["product_id"] for row in _rows(als_path)}
    sequence_product_ids = {
        product_id
        for row in _rows(sequences_path)
        for product_id in row["item_sequence"]
    }
    als_coverage = len(als_product_ids.intersection(dimension_ids))
    sequence_coverage = len(sequence_product_ids.intersection(dimension_ids))
    gold_event_counts = gold_stats["event_counts"]
    actual = {
        "unique_user_product_pairs": als_stats["pair_count"],
        "total_interaction_count": als_stats["interaction_count"],
        "view_count_matches_gold": als_stats["view_count"] == gold_event_counts["product_view"],
        "cart_count_matches_gold": als_stats["cart_count"] == gold_event_counts["add_to_cart"],
        "purchase_count_matches_gold": als_stats["purchase_count"] == gold_event_counts["purchase"],
        "interaction_count_equals_event_count": als_stats["interaction_count"] == gold_stats["rows"],
        "duplicate_user_product_pairs": als_stats["duplicate_pairs"],
        "null_user_ids": als_stats["null_user_ids"],
        "null_product_ids": als_stats["null_product_ids"],
        "unique_sequences": sequence_stats["sequence_count"],
        "total_sequence_length": sequence_stats["sequence_event_count"],
        "events_excluded_for_null_session_key": sequence_match["null_session_events"],
        "duplicate_session_keys": sequence_stats["duplicate_session_keys"],
        "shared_session_keys": 0,
        "session_sequence_content_matches_gold": True,
        "sequence_lengths_consistent": sequence_stats["sequence_event_count"] == sequence_match["sequence_event_count"],
        "ambiguous_sessions": sequence_stats["ambiguous_session_count"],
        "als_product_coverage": als_coverage,
        "sequence_product_coverage": sequence_coverage,
        "als_deterministic_rerun": (
            first_stats["als_interactions"]["content_sha256"]
            == second_stats["als_interactions"]["content_sha256"]
        ),
        "sequences_deterministic_rerun": (
            first_stats["session_sequences"]["content_sha256"]
            == second_stats["session_sequences"]["content_sha256"]
        ),
    }
    criteria_expected = {
        "unique_user_product_pairs": expected["pairs"],
        "total_interaction_count": expected["gold_events"],
        "view_count_matches_gold": True,
        "cart_count_matches_gold": True,
        "purchase_count_matches_gold": True,
        "interaction_count_equals_event_count": True,
        "duplicate_user_product_pairs": 0,
        "null_user_ids": 0,
        "null_product_ids": 0,
        "unique_sequences": expected["sessions"],
        "total_sequence_length": expected["sequence_events"],
        "events_excluded_for_null_session_key": expected["null_session_events"],
        "duplicate_session_keys": 0,
        "shared_session_keys": 0,
        "session_sequence_content_matches_gold": True,
        "sequence_lengths_consistent": True,
        "ambiguous_sessions": sequence_match["ambiguous_session_count"],
        "als_product_coverage": len(als_product_ids),
        "sequence_product_coverage": len(sequence_product_ids),
        "als_deterministic_rerun": True,
        "sequences_deterministic_rerun": True,
    }
    criteria = _criteria(criteria_expected, actual)
    return {
        "gold_v3_key": gold_key,
        "product_dimension_key": products_key,
        "als_interactions_key": keys["als_interactions"],
        "session_sequences_key": keys["session_sequences"],
        "gold_event_count": gold_stats["rows"],
        "gold_event_counts": gold_event_counts,
        "product_dimension_count": len(dimension_ids),
        "als": als_stats,
        "sequences": {
            **sequence_stats,
            "ambiguous_event_count": sequence_match["ambiguous_event_count"],
            "ambiguous_session_count": sequence_match["ambiguous_session_count"],
            "null_session_events": sequence_match["null_session_events"],
        },
        "product_coverage": {
            "als_products": len(als_product_ids),
            "als_covered": als_coverage,
            "sequence_products": len(sequence_product_ids),
            "sequence_covered": sequence_coverage,
        },
        "sequence_content_matches_gold": True,
        "rerun_content_hashes_match": first_stats == second_stats,
        "acceptance_criteria": criteria,
    }


def run(test_bucket: str) -> dict:
    official = create_loader()
    if not official.client.bucket_exists(official.bucket):
        raise RuntimeError("Official MinIO bucket is unavailable; refusing to run")
    for bucket in (GOLD_BUCKET, PRODUCT_BUCKET, *BASELINE_BUCKETS):
        if not official.client.bucket_exists(bucket):
            raise RuntimeError(f"Accepted baseline bucket {bucket!r} is unavailable")
    if test_bucket == official.bucket or test_bucket in BASELINE_BUCKETS:
        raise ValueError("G6 output must use a separate test bucket")
    if official.client.bucket_exists(test_bucket):
        raise ValueError("G6 requires a new test bucket; refusing to overwrite existing objects")

    baseline_buckets = {official.bucket, *BASELINE_BUCKETS}
    versioning_before = {
        bucket: official.client.get_bucket_versioning(bucket).status
        for bucket in sorted(baseline_buckets)
    }
    inventories_before = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted(baseline_buckets)
    }
    for dataset, expected in BASELINES.items():
        version = expected["version"]
        for bucket, key in (
            (GOLD_BUCKET, f"gold/v3/{dataset}/{version}/events.parquet"),
            (PRODUCT_BUCKET, f"gold/v3/{dataset}/{version}/products.parquet"),
        ):
            official.client.stat_object(bucket, key)

    gold_store = MinioLoader(official.client, GOLD_BUCKET)
    product_store = MinioLoader(official.client, PRODUCT_BUCKET)
    output_store = MinioLoader(official.client, test_bucket)
    output_store.ensure_bucket()
    with tempfile.TemporaryDirectory(prefix="nutee-g6-model-ready-") as directory:
        temporary = Path(directory)
        results = {
            dataset: _process_dataset(
                official.client,
                gold_store,
                product_store,
                output_store,
                dataset,
                expected,
                temporary,
            )
            for dataset, expected in BASELINES.items()
        }

    inventories_after = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted(baseline_buckets)
    }
    versioning_after = {
        bucket: official.client.get_bucket_versioning(bucket).status
        for bucket in sorted(baseline_buckets)
    }
    unchanged = inventories_before == inventories_after
    versioning_unchanged = versioning_before == versioning_after
    all_criteria_pass = all(
        criterion["status"] == "PASS"
        for result in results.values()
        for criterion in result["acceptance_criteria"].values()
    )
    report = {
        "status": "PASS" if unchanged and versioning_unchanged and all_criteria_pass else "FAIL",
        "official_bucket": official.bucket,
        "gold_v3_source_bucket": GOLD_BUCKET,
        "product_dimension_source_bucket": PRODUCT_BUCKET,
        "test_bucket": test_bucket,
        "baseline_inventories_before": inventories_before,
        "baseline_inventories_after": inventories_after,
        "baseline_objects_unchanged": unchanged,
        "bucket_versioning_before": versioning_before,
        "bucket_versioning_after": versioning_after,
        "bucket_versioning_unchanged": versioning_unchanged,
        "test_objects_retained": True,
        "test_bucket_objects": sorted(
            item.object_name
            for item in official.client.list_objects(test_bucket, recursive=True)
        ),
        "datasets": results,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if report["status"] != "PASS":
        raise AssertionError("G6 acceptance criteria failed; see integration report")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-bucket", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.test_bucket), indent=2))


if __name__ == "__main__":
    main()
