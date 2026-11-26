"""Verify sequential Gold V3 in an isolated MinIO bucket."""

import argparse
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from loader.common.minio_store import MinioLoader
from pipeline import build_gold, create_loader
from transformer.common.gold import (
    GOLD_SCHEMA,
    GOLD_SCHEMA_V3,
    order_gold_v3,
    validate_gold,
)
from transformer.common.silver import BATCH_SIZE


BASELINES = {
    "rees46_electronics": {
        "version": "cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7",
        "bucket": "nutee-session-v2-test-20261126b",
        "events": 885129,
        "unique_session_keys": 490633,
        "positioned_events": 884964,
        "missing_positions": 165,
        "timestamp_tie_groups": 3294,
        "ambiguous_events": 7939,
        "source_silver_rows": 885129,
        "adapter": "rees46_events",
    },
    "merrec": {
        "version": "3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca",
        "bucket": "nutee-event-preservation-g3-test-20261126",
        "events": 515601,
        "unique_session_keys": 112859,
        "positioned_events": 515601,
        "missing_positions": 0,
        "timestamp_tie_groups": 178,
        "ambiguous_events": 393,
        "source_silver_rows": 605269,
        "adapter": "merrec_events",
    },
}
G2_TEST_BUCKET = "nutee-session-v2-test-20261126b"
G3_TEST_BUCKET = "nutee-event-preservation-g3-test-20261126"
NON_POSITION_FIELDS = [name for name in GOLD_SCHEMA.names if name != "position_in_session"]
REPORT_PATH = Path(__file__).resolve().parents[2] / "reports" / "sequential_integrity_g4" / "integration.json"


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


def _row_digest(row: dict, columns: list[str]) -> bytes:
    encoded = json.dumps(
        [row[name] for name in columns],
        ensure_ascii=False,
        separators=(",", ":"),
        default=lambda value: value.isoformat() if isinstance(value, datetime) else str(value),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).digest()


def _compare_multisets(connection, old_path: Path, new_path: Path, fields: list[str], table: str) -> bool:
    connection.execute(
        f"CREATE TABLE {table} (side TEXT, digest BLOB, occurrences INTEGER, PRIMARY KEY(side, digest))"
    )
    for side, path in (("old", old_path), ("new", new_path)):
        with pq.ParquetFile(path) as parquet:
            for batch in parquet.iter_batches(batch_size=BATCH_SIZE, columns=fields):
                counts = Counter(_row_digest(row, fields) for row in batch.to_pylist())
                connection.executemany(
                    f"INSERT INTO {table}(side, digest, occurrences) VALUES (?, ?, ?) "
                    f"ON CONFLICT(side, digest) DO UPDATE SET occurrences = occurrences + excluded.occurrences",
                    [(side, digest, count) for digest, count in counts.items()],
                )
                connection.commit()
    mismatch = connection.execute(
        f"SELECT COUNT(*) FROM {table} AS old "
        f"LEFT JOIN (SELECT digest, occurrences FROM {table} WHERE side = 'new') AS new USING (digest) "
        "WHERE old.side = 'old' AND (new.digest IS NULL OR old.occurrences != new.occurrences)"
    ).fetchone()[0]
    additional = connection.execute(
        f"SELECT COUNT(*) FROM {table} AS new "
        f"LEFT JOIN (SELECT digest, occurrences FROM {table} WHERE side = 'old') AS old USING (digest) "
        "WHERE new.side = 'new' AND old.digest IS NULL"
    ).fetchone()[0]
    return mismatch == 0 and additional == 0


def _reverse_row_groups(source: Path, destination: Path) -> bool:
    with pq.ParquetFile(source) as parquet:
        group_count = parquet.metadata.num_row_groups
        if group_count > 1:
            with pq.ParquetWriter(destination, GOLD_SCHEMA, compression="snappy") as writer:
                for index in reversed(range(group_count)):
                    writer.write_table(parquet.read_row_group(index))
        else:
            table = parquet.read()
            reversed_indices = pa.array(range(table.num_rows - 1, -1, -1), type=pa.int64())
            with pq.ParquetWriter(destination, GOLD_SCHEMA, compression="snappy") as writer:
                writer.write_table(table.take(reversed_indices))
    return group_count > 1 or table.num_rows > 1


def _event_id_order_digest(path: Path) -> str:
    digest = sha256()
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE, columns=["event_id"]):
            for event_id in batch.column(0).to_pylist():
                digest.update(event_id.encode("utf-8") + b"\n")
    return digest.hexdigest()


def _session_metrics(path: Path) -> dict:
    rows = 0
    unique_keys = set()
    positioned = 0
    missing_positions = 0
    null_sessions = 0
    shared_keys = 0
    tie_groups = 0
    ambiguous_events = 0
    event_counts = Counter()
    current_session = None
    current_user = None
    current_position = 0
    previous_time = None
    null_time_seen = False
    seen_null_session = False
    tie_key = None
    tie_count = 0
    tie_all_ambiguous = True
    tie_last_event_id = None

    def finish_tie_group():
        nonlocal tie_groups, ambiguous_events, tie_key, tie_count, tie_all_ambiguous, tie_last_event_id
        if tie_key is None:
            return
        if tie_count > 1:
            if not tie_all_ambiguous:
                raise AssertionError("Timestamp tie group is not fully marked ambiguous")
            tie_groups += 1
            ambiguous_events += tie_count
        elif tie_all_ambiguous:
            raise AssertionError("A non-tied event is marked order_ambiguous")
        tie_key = None
        tie_count = 0
        tie_all_ambiguous = True
        tie_last_event_id = None

    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(
            batch_size=BATCH_SIZE,
            columns=[
                "event_id", "event_type", "event_time", "user_id",
                "session_key", "position_in_session", "order_ambiguous",
            ],
        ):
            for row in batch.to_pylist():
                rows += 1
                event_counts[row["event_type"]] += 1
                session_key = row["session_key"]
                event_time = row["event_time"]
                position = row["position_in_session"]
                ambiguous = row["order_ambiguous"]

                if session_key is None:
                    finish_tie_group()
                    seen_null_session = True
                    null_sessions += 1
                    missing_positions += 1
                    if position is not None or ambiguous is not None:
                        raise AssertionError("Null session_key must have null position and ambiguity")
                    current_session = None
                    current_user = None
                    current_position = 0
                    previous_time = None
                    null_time_seen = False
                    continue
                if seen_null_session:
                    raise AssertionError("Non-null session appeared after null-session rows")

                if session_key != current_session:
                    finish_tie_group()
                    if current_session is not None and session_key < current_session:
                        raise AssertionError("Gold sessions are not sorted by session_key")
                    current_session = session_key
                    current_user = row["user_id"]
                    current_position = 1
                    previous_time = None
                    null_time_seen = False
                    unique_keys.add(session_key)
                else:
                    if current_user != row["user_id"]:
                        shared_keys += 1
                    current_position += 1

                if position != current_position:
                    raise AssertionError("position_in_session is not continuous from 1")
                if isinstance(position, bool) or not isinstance(position, int):
                    raise AssertionError("position_in_session must be an integer")
                positioned += 1
                if not isinstance(ambiguous, bool):
                    raise AssertionError("Non-null sessions require boolean order_ambiguous")

                if event_time is None:
                    finish_tie_group()
                    null_time_seen = True
                    if ambiguous:
                        raise AssertionError("A missing timestamp cannot be marked as a timestamp tie")
                else:
                    if null_time_seen:
                        raise AssertionError("A non-null timestamp follows a null timestamp in a session")
                    if previous_time is not None and event_time < previous_time:
                        raise AssertionError("Timestamps decrease within a session")
                    pair = (session_key, event_time)
                    if pair != tie_key:
                        finish_tie_group()
                        tie_key = pair
                        tie_count = 1
                        tie_all_ambiguous = ambiguous
                        tie_last_event_id = row["event_id"]
                    else:
                        if tie_last_event_id is not None and row["event_id"] < tie_last_event_id:
                            raise AssertionError("event_id tie-break order is not ascending")
                        tie_count += 1
                        tie_all_ambiguous = tie_all_ambiguous and ambiguous
                        tie_last_event_id = row["event_id"]
                previous_time = event_time
    finish_tie_group()
    return {
        "rows": rows,
        "event_counts": dict(event_counts),
        "unique_session_keys": len(unique_keys),
        "positioned_events": positioned,
        "missing_positions": missing_positions,
        "null_session_keys": null_sessions,
        "timestamp_tie_groups": tie_groups,
        "ambiguous_events": ambiguous_events,
        "shared_session_keys": shared_keys,
    }


def _process_dataset(client, test_store, dataset: str, expected: dict, temporary: Path) -> dict:
    version = expected["version"]
    source_bucket = expected["bucket"]
    silver_key = f"silver/{dataset}/{expected['adapter']}/{version}/events.parquet"
    v2_key = f"gold/v2/{dataset}/{version}/events.parquet"
    v3_key = f"gold/v3/{dataset}/{version}/events.parquet"
    silver_path = temporary / f"{dataset}-silver.parquet"
    v2_path = temporary / f"{dataset}-gold-v2.parquet"
    v3_path = temporary / f"{dataset}-gold-v3.parquet"
    shuffled_v2_path = temporary / f"{dataset}-gold-v2-shuffled.parquet"
    shuffled_v3_path = temporary / f"{dataset}-gold-v3-shuffled.parquet"

    client.fget_object(source_bucket, silver_key, str(silver_path))
    client.fget_object(source_bucket, v2_key, str(v2_path))
    with pq.ParquetFile(silver_path) as silver:
        silver_rows = silver.metadata.num_rows
    v2_stats = validate_gold(v2_path, dataset, schema=GOLD_SCHEMA)
    if v2_stats["rows"] != expected["events"] or silver_rows != expected["source_silver_rows"]:
        raise AssertionError(f"{dataset} Gold V2 or Silver baseline row count differs")

    test_store.load(silver_key, silver_path, "application/vnd.apache.parquet")
    _, generated_stats = build_gold(
        test_store,
        {"name": dataset, "adapter": expected["adapter"]},
        version,
        schema=GOLD_SCHEMA_V3,
    )
    client.fget_object(test_store.bucket, v3_key, str(v3_path))
    v3_stats = validate_gold(v3_path, dataset, schema=GOLD_SCHEMA_V3)
    if generated_stats != v3_stats:
        raise AssertionError("Gold V3 readback differs from generated output")

    connection = sqlite3.connect(temporary / f"{dataset}-multisets.sqlite")
    try:
        fields_match = _compare_multisets(
            connection,
            v2_path,
            v3_path,
            NON_POSITION_FIELDS,
            "gold_non_position_fields",
        )
        ids_match = _compare_multisets(
            connection,
            v2_path,
            v3_path,
            ["event_id"],
            "gold_event_ids",
        )
    finally:
        connection.close()

    metrics = _session_metrics(v3_path)
    if not _reverse_row_groups(v2_path, shuffled_v2_path):
        raise AssertionError("Gold V2 baseline could not be reordered for determinism test")
    input_order_changed = _event_id_order_digest(v2_path) != _event_id_order_digest(shuffled_v2_path)
    if not input_order_changed:
        raise AssertionError("The Gold V2 event input order did not change")
    shuffled_stats = order_gold_v3(shuffled_v2_path, shuffled_v3_path, dataset)
    if shuffled_stats["content_sha256"] != v3_stats["content_sha256"]:
        raise AssertionError("Gold V3 result depends on Gold V2 input row order")

    actual = {
        "gold_events": metrics["rows"],
        "unique_session_keys": metrics["unique_session_keys"],
        "events_with_position": metrics["positioned_events"],
        "events_missing_position": metrics["missing_positions"],
        "timestamp_tie_groups": metrics["timestamp_tie_groups"],
        "ambiguous_events": metrics["ambiguous_events"],
        "shared_session_keys": metrics["shared_session_keys"],
        "events_lost_or_added": 0 if fields_match else -1,
        "event_ids_changed": 0 if ids_match else -1,
    }
    criteria_expected = {
        "gold_events": expected["events"],
        "unique_session_keys": expected["unique_session_keys"],
        "events_with_position": expected["positioned_events"],
        "events_missing_position": expected["missing_positions"],
        "timestamp_tie_groups": expected["timestamp_tie_groups"],
        "ambiguous_events": expected["ambiguous_events"],
        "shared_session_keys": 0,
        "events_lost_or_added": 0,
        "event_ids_changed": 0,
    }
    criteria = {
        name: {
            "expected": criteria_expected[name],
            "actual": actual[name],
            "status": "PASS" if criteria_expected[name] == actual[name] else "FAIL",
        }
        for name in criteria_expected
    }
    return {
        "silver_key": silver_key,
        "gold_v2_baseline_bucket": source_bucket,
        "gold_v2_baseline_key": v2_key,
        "gold_v3_test_key": v3_key,
        "silver_rows": silver_rows,
        "gold_v2_rows": v2_stats["rows"],
        "gold_v3_rows": v3_stats["rows"],
        "gold_v2_content_sha256": v2_stats["content_sha256"],
        "gold_v3_content_sha256": v3_stats["content_sha256"],
        "gold_non_position_field_multiset_matches": fields_match,
        "gold_event_id_multiset_matches": ids_match,
        "reordered_input_rows": input_order_changed,
        "reordered_input_gold_v3_content_sha256": shuffled_stats["content_sha256"],
        "output_is_independent_of_input_order": shuffled_stats["content_sha256"] == v3_stats["content_sha256"],
        "metrics": metrics,
        "acceptance_criteria": criteria,
    }


def run(test_bucket: str) -> dict:
    official = create_loader()
    if not official.client.bucket_exists(official.bucket):
        raise RuntimeError("Official MinIO bucket is unavailable; refusing to run")
    if test_bucket == official.bucket or official.client.bucket_exists(test_bucket):
        raise ValueError("GĐ4 requires a new test bucket distinct from the official bucket")
    if test_bucket in {G2_TEST_BUCKET, G3_TEST_BUCKET}:
        raise ValueError("GĐ4 test bucket must not overwrite a GĐ2/GĐ3 baseline")
    for item in BASELINES.values():
        if not official.client.bucket_exists(item["bucket"]):
            raise RuntimeError(f"Accepted baseline bucket {item['bucket']} is unavailable")

    official_versioning_before = official.client.get_bucket_versioning(official.bucket).status
    official_before = _snapshot_summary(official.client, official.bucket)
    baseline_before = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted({item["bucket"] for item in BASELINES.values()})
    }
    for dataset, item in BASELINES.items():
        version = item["version"]
        for key in (
            f"silver/{dataset}/{item['adapter']}/{version}/events.parquet",
            f"gold/v2/{dataset}/{version}/events.parquet",
        ):
            official.client.stat_object(item["bucket"], key)

    test_store = MinioLoader(official.client, test_bucket)
    test_store.ensure_bucket()
    results = {}
    with tempfile.TemporaryDirectory(prefix="nutee-g4-sequential-") as directory:
        temporary = Path(directory)
        for dataset, expected in BASELINES.items():
            results[dataset] = _process_dataset(official.client, test_store, dataset, expected, temporary)

    official_after = _snapshot_summary(official.client, official.bucket)
    baseline_after = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted({item["bucket"] for item in BASELINES.values()})
    }
    official_versioning_after = official.client.get_bucket_versioning(official.bucket).status
    data_unchanged = official_before == official_after and baseline_before == baseline_after
    versioning_unchanged = official_versioning_before == official_versioning_after
    all_criteria_pass = all(
        entry["status"] == "PASS"
        for result in results.values()
        for entry in result["acceptance_criteria"].values()
    )
    report = {
        "status": "PASS" if data_unchanged and versioning_unchanged and all_criteria_pass else "FAIL",
        "official_bucket": official.bucket,
        "test_bucket": test_bucket,
        "official_inventory_before": official_before,
        "official_inventory_after": official_after,
        "baseline_inventories_before": baseline_before,
        "baseline_inventories_after": baseline_after,
        "official_bucket_versioning_before": official_versioning_before,
        "official_bucket_versioning_after": official_versioning_after,
        "official_and_baseline_objects_unchanged": data_unchanged,
        "bucket_versioning_unchanged": versioning_unchanged,
        "test_objects_retained": True,
        "datasets": results,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if report["status"] != "PASS":
        raise AssertionError("One or more GĐ4 acceptance criteria failed; see integration report")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-bucket", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.test_bucket), indent=2))


if __name__ == "__main__":
    main()
