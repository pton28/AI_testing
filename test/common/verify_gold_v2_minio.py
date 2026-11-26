"""Build and compare Gold V2 in a dedicated MinIO test bucket."""

import argparse
import json
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import datetime
from hashlib import sha256
from itertools import zip_longest
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa

from pipeline import build_gold, create_loader
from loader.common.minio_store import MinioLoader
from transformer.common.gold import (
    GOLD_SCHEMA,
    GOLD_SCHEMA_V1,
    build_session_key,
    validate_gold,
)
from transformer.common.silver import BATCH_SIZE


EXPECTED = {
    "rees46_electronics": {"rows": 885129, "session_keys": 490633, "null_session_keys": 165},
    "merrec": {"rows": 515601, "session_keys": 112859, "null_session_keys": 0},
}
LEGACY_FIELDS = GOLD_SCHEMA_V1.names
REPORT_PATH = Path(__file__).resolve().parents[2] / "reports" / "session_integrity_g2" / "minio_integration.json"


class IntegrationBlocked(Exception):
    pass


def _object_snapshot(client, bucket: str) -> dict:
    snapshot = {}
    for dataset in EXPECTED:
        for prefix in (
            f"bronze/{dataset}/",
            f"silver/{dataset}/",
            f"gold/{dataset}/",
            f"gold/v2/{dataset}/",
        ):
            for item in client.list_objects(bucket, prefix=prefix, recursive=True):
                snapshot[item.object_name] = {
                    "etag": item.etag,
                    "size": item.size,
                    "last_modified": item.last_modified.isoformat() if item.last_modified else None,
                    "version_id": item.version_id,
                }
    return snapshot


def _snapshot_digest(snapshot: dict) -> str:
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _row_digest(row: dict) -> bytes:
    encoded = json.dumps(
        [row[name] for name in LEGACY_FIELDS],
        ensure_ascii=False,
        separators=(",", ":"),
        default=lambda value: value.isoformat() if isinstance(value, datetime) else str(value),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).digest()


def _add_multiset_rows(connection, table: str, side: str, path: Path) -> int:
    rows = 0
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE, columns=LEGACY_FIELDS):
            counts = Counter(_row_digest(row) for row in batch.to_pylist())
            connection.executemany(
                f"INSERT INTO {table}(side, digest, occurrences) VALUES (?, ?, ?) "
                "ON CONFLICT(side, digest) DO UPDATE SET occurrences = occurrences + excluded.occurrences",
                [(side, digest, occurrences) for digest, occurrences in counts.items()],
            )
            rows += batch.num_rows
    connection.commit()
    return rows


def _compare_legacy_multisets(connection) -> bool:
    mismatches = connection.execute(
        "SELECT COUNT(*) FROM v1_rows AS old "
        "LEFT JOIN v2_rows AS new USING (digest) "
        "WHERE new.digest IS NULL OR old.occurrences != new.occurrences"
    ).fetchone()[0]
    additional = connection.execute(
        "SELECT COUNT(*) FROM v2_rows AS new "
        "LEFT JOIN v1_rows AS old USING (digest) "
        "WHERE old.digest IS NULL"
    ).fetchone()[0]
    return mismatches == 0 and additional == 0


def _compare_ordered_ids(v1_path: Path, v2_path: Path) -> tuple[int, int]:
    event_id_changes = 0
    session_id_changes = 0
    with pq.ParquetFile(v1_path) as v1, pq.ParquetFile(v2_path) as v2:
        left = v1.iter_batches(batch_size=BATCH_SIZE, columns=["event_id", "session_id"])
        right = v2.iter_batches(batch_size=BATCH_SIZE, columns=["event_id", "session_id"])
        for old_batch, new_batch in zip_longest(left, right):
            if old_batch is None or new_batch is None or old_batch.num_rows != new_batch.num_rows:
                raise AssertionError("Gold V1 and V2 row ordering/count differs")
            old_ids = old_batch.column(0).to_pylist()
            new_ids = new_batch.column(0).to_pylist()
            old_sessions = old_batch.column(1).to_pylist()
            new_sessions = new_batch.column(1).to_pylist()
            event_id_changes += sum(old != new for old, new in zip(old_ids, new_ids))
            session_id_changes += sum(old != new for old, new in zip(old_sessions, new_sessions))
    return event_id_changes, session_id_changes


def _profile_session_keys(path: Path, database: Path) -> dict:
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE session_keys ("
        "session_key TEXT PRIMARY KEY, source_dataset TEXT NOT NULL, "
        "user_id TEXT NOT NULL, session_id TEXT NOT NULL, "
        "UNIQUE(source_dataset, user_id, session_id))"
    )
    null_keys = 0
    try:
        with pq.ParquetFile(path) as parquet:
            for batch in parquet.iter_batches(
                batch_size=BATCH_SIZE,
                columns=["source_dataset", "user_id", "session_id", "session_key"],
            ):
                entries = {}
                for row in batch.to_pylist():
                    expected = build_session_key(row["source_dataset"], row["user_id"], row["session_id"])
                    if row["session_key"] != expected:
                        raise AssertionError("Gold V2 session_key does not match its source tuple")
                    if expected is None:
                        null_keys += 1
                        continue
                    identity = (row["source_dataset"], row["user_id"], row["session_id"])
                    previous = entries.get(expected)
                    if previous is not None and previous != identity:
                        raise AssertionError("SHA-256 collision between distinct session tuples")
                    entries[expected] = identity

                with connection:
                    for key, identity in entries.items():
                        cursor = connection.execute(
                            "INSERT OR IGNORE INTO session_keys VALUES (?, ?, ?, ?)",
                            (key, *identity),
                        )
                        if cursor.rowcount == 0:
                            by_key = connection.execute(
                                "SELECT source_dataset, user_id, session_id "
                                "FROM session_keys WHERE session_key = ?",
                                (key,),
                            ).fetchone()
                            if by_key == identity:
                                continue
                            by_tuple = connection.execute(
                                "SELECT session_key FROM session_keys "
                                "WHERE source_dataset = ? AND user_id = ? AND session_id = ?",
                                identity,
                            ).fetchone()
                            if by_tuple:
                                raise AssertionError("One session tuple maps to multiple session keys")
                            raise AssertionError("SHA-256 collision between distinct session tuples")

        unique_keys = connection.execute("SELECT COUNT(*) FROM session_keys").fetchone()[0]
        shared_users = connection.execute(
            "SELECT COUNT(*) FROM ("
            "SELECT session_key FROM session_keys GROUP BY session_key "
            "HAVING COUNT(DISTINCT user_id) > 1)"
        ).fetchone()[0]
        return {
            "unique_non_null_session_keys": unique_keys,
            "null_session_keys": null_keys,
            "session_keys_shared_by_users": shared_users,
            "hash_collisions": 0,
            "tuple_to_key_mappings_checked": unique_keys,
        }
    finally:
        connection.close()


def _dataset_result(official, test_store, dataset: dict, version: str, temporary: Path) -> dict:
    name = dataset["name"]
    expected = EXPECTED[name]
    silver_key = f"silver/{name}/{dataset['adapter']}/{version}/events.parquet"
    v1_key = f"gold/{name}/{version}/events.parquet"
    silver_path = temporary / f"{name}-silver.parquet"
    v1_path = temporary / f"{name}-gold-v1.parquet"
    v2_path = temporary / f"{name}-gold-v2.parquet"

    official.client.fget_object(official.bucket, silver_key, str(silver_path))
    official.client.fget_object(official.bucket, v1_key, str(v1_path))
    with pq.ParquetFile(silver_path) as silver:
        silver_rows = silver.metadata.num_rows
    v1_stats = validate_gold(v1_path, name, schema=GOLD_SCHEMA_V1)
    if v1_stats["rows"] != expected["rows"] or silver_rows != expected["rows"]:
        raise AssertionError(f"{name} row count does not match the approved snapshot")

    test_store.load(silver_key, silver_path, "application/vnd.apache.parquet")
    v2_key, generated_stats = build_gold(test_store, dataset, version)
    test_store.client.fget_object(test_store.bucket, v2_key, str(v2_path))
    v2_stats = validate_gold(v2_path, name, schema=GOLD_SCHEMA)
    if v2_stats != generated_stats:
        raise AssertionError("Gold V2 readback differs from generated output")
    if v1_stats["event_counts"] != v2_stats["event_counts"]:
        raise AssertionError("Gold V1 and V2 event distributions differ")
    if v2_stats["rows"] != expected["rows"]:
        raise AssertionError(f"{name} Gold V2 row count does not match the approved snapshot")

    connection = sqlite3.connect(temporary / f"{name}-comparison.sqlite")
    try:
        connection.execute(
            "CREATE TABLE multiset (side TEXT, digest BLOB, occurrences INTEGER, PRIMARY KEY(side,digest))"
        )
        _add_multiset_rows(connection, "multiset", "v1", v1_path)
        _add_multiset_rows(connection, "multiset", "v2", v2_path)
        # Split the shared table into temporary views to compare both multisets.
        connection.execute("CREATE TEMP VIEW v1_rows AS SELECT digest, occurrences FROM multiset WHERE side='v1'")
        connection.execute("CREATE TEMP VIEW v2_rows AS SELECT digest, occurrences FROM multiset WHERE side='v2'")
        legacy_multiset_matches = _compare_legacy_multisets(connection)
    finally:
        connection.close()

    if not legacy_multiset_matches:
        raise AssertionError("The multiset of the 11 legacy Gold fields changed")
    event_id_changes, session_id_changes = _compare_ordered_ids(v1_path, v2_path)
    if event_id_changes or session_id_changes:
        raise AssertionError("event_id or original session_id changed in Silver processing order")

    with pq.ParquetFile(v2_path) as gold:
        if not gold.schema_arrow.equals(GOLD_SCHEMA, check_metadata=False):
            raise AssertionError("Gold V2 schema mismatch")
        legacy_schema = pa.schema([gold.schema_arrow.field(name) for name in LEGACY_FIELDS])
        if not legacy_schema.equals(GOLD_SCHEMA_V1, check_metadata=False):
            raise AssertionError("Gold V2 legacy field schema differs from V1")
    session_profile = _profile_session_keys(v2_path, temporary / f"{name}-keys.sqlite")
    if session_profile["unique_non_null_session_keys"] != expected["session_keys"]:
        raise AssertionError("Unique session_key count does not match acceptance criteria")
    if session_profile["null_session_keys"] != expected["null_session_keys"]:
        raise AssertionError("Null session_key count does not match acceptance criteria")
    if session_profile["session_keys_shared_by_users"] != 0:
        raise AssertionError("A session_key is associated with multiple users")

    events_lost = max(v1_stats["rows"] - v2_stats["rows"], 0)
    events_added = max(v2_stats["rows"] - v1_stats["rows"], 0)
    criteria = {
        "gold_events": {"expected": expected["rows"], "actual": v2_stats["rows"]},
        "unique_non_null_session_keys": {
            "expected": expected["session_keys"],
            "actual": session_profile["unique_non_null_session_keys"],
        },
        "null_session_keys": {
            "expected": expected["null_session_keys"],
            "actual": session_profile["null_session_keys"],
        },
        "session_keys_shared_by_users": {
            "expected": 0,
            "actual": session_profile["session_keys_shared_by_users"],
        },
        "events_lost": {"expected": 0, "actual": events_lost},
        "events_added": {"expected": 0, "actual": events_added},
        "event_ids_changed": {"expected": 0, "actual": event_id_changes},
        "original_session_ids_changed": {"expected": 0, "actual": session_id_changes},
        "hash_collisions": {"expected": 0, "actual": session_profile["hash_collisions"]},
        "legacy_11_field_multiset_matches": {"expected": True, "actual": legacy_multiset_matches},
    }
    for criterion in criteria.values():
        criterion["status"] = "PASS" if criterion["actual"] == criterion["expected"] else "FAIL"

    return {
        "silver_key": silver_key,
        "gold_v1_key": v1_key,
        "gold_v2_test_key": v2_key,
        "rows": {"expected": expected["rows"], "silver": silver_rows, "gold_v1": v1_stats["rows"],
                 "gold_v2": v2_stats["rows"]},
        "event_type_distribution_matches": v1_stats["event_counts"] == v2_stats["event_counts"],
        "legacy_11_field_multiset_matches": legacy_multiset_matches,
        "event_ids_changed_in_processing_order": event_id_changes,
        "original_session_ids_changed_in_processing_order": session_id_changes,
        "events_lost": events_lost,
        "events_added": events_added,
        "acceptance_criteria": criteria,
        **session_profile,
    }


def _run(args) -> dict:
    try:
        official = create_loader()
    except Exception as error:
        raise IntegrationBlocked(f"Official MinIO configuration/unavailable ({type(error).__name__})") from error

    if args.test_bucket == official.bucket:
        raise IntegrationBlocked("Test bucket must be different from the configured official bucket")
    try:
        if not official.client.bucket_exists(official.bucket):
            raise IntegrationBlocked("Configured official MinIO bucket is unavailable")
        official_before = _object_snapshot(official.client, official.bucket)
        versioning_before = official.client.get_bucket_versioning(official.bucket).status
    except IntegrationBlocked:
        raise
    except Exception as error:
        raise IntegrationBlocked(f"Official MinIO is unavailable for read-only inspection ({type(error).__name__})") from error

    versions = {
        "rees46_electronics": args.rees46_source_version,
        "merrec": args.merrec_source_version,
    }
    for name, version in versions.items():
        if len(version) != 64 or any(char not in "0123456789abcdef" for char in version):
            raise IntegrationBlocked(f"{name} source version must be a lowercase SHA-256")

    config_path = Path(__file__).resolve().parents[2] / "data" / "datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    datasets = {item["name"]: item for item in config["datasets"]}

    for name, version in versions.items():
        dataset = datasets[name]
        required = (
            f"silver/{name}/{dataset['adapter']}/{version}/events.parquet",
            f"gold/{name}/{version}/events.parquet",
        )
        if any(key not in official_before for key in required):
            raise IntegrationBlocked(f"Official Silver or Gold V1 snapshot missing for {name}")

    try:
        test_bucket_exists = official.client.bucket_exists(args.test_bucket)
        if not test_bucket_exists:
            official.client.make_bucket(args.test_bucket)
    except Exception as error:
        raise IntegrationBlocked(f"Could not provision the explicit test bucket ({type(error).__name__})") from error

    try:
        existing_test_objects = list(official.client.list_objects(args.test_bucket, recursive=True))
    except Exception as error:
        raise IntegrationBlocked(f"Test bucket is unavailable ({type(error).__name__})") from error
    if existing_test_objects:
        raise IntegrationBlocked("Test bucket is not empty; refusing to overwrite or delete test objects")

    test_store = MinioLoader(official.client, args.test_bucket)
    report = {
        "status": "RUNNING",
        "official_bucket": official.bucket,
        "test_bucket": args.test_bucket,
        "official_bucket_versioning_before": versioning_before,
        "official_object_count_before": len(official_before),
        "official_inventory_sha256_before": _snapshot_digest(official_before),
        "datasets": {},
        "test_objects_retained": True,
    }
    with tempfile.TemporaryDirectory(prefix="nutee-gold-v2-acceptance-") as directory:
        temporary = Path(directory)
        for name, version in versions.items():
            report["datasets"][name] = _dataset_result(
                official, test_store, datasets[name], version, temporary
            )

    official_after = _object_snapshot(official.client, official.bucket)
    versioning_after = official.client.get_bucket_versioning(official.bucket).status
    report["official_objects_unchanged"] = official_before == official_after
    report["official_object_count_after"] = len(official_after)
    report["official_inventory_sha256_after"] = _snapshot_digest(official_after)
    report["official_bucket_versioning_unchanged"] = versioning_before == versioning_after
    if not report["official_objects_unchanged"] or not report["official_bucket_versioning_unchanged"]:
        raise AssertionError("Official MinIO object inventory or versioning changed during the test")
    report["status"] = "PASS"
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-bucket", required=True, help="Dedicated test bucket; never use the official bucket")
    parser.add_argument("--rees46-source-version", required=True)
    parser.add_argument("--merrec-source-version", required=True)
    args = parser.parse_args()
    report = {}
    try:
        report = _run(args)
    except IntegrationBlocked as error:
        report = {
            "status": "BLOCKED",
            "test_bucket": args.test_bucket,
            "reason": str(error),
            "test_objects_retained": True,
        }
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2), file=sys.stderr)
        raise SystemExit(2) from error
    except Exception as error:
        report.update(status="FAIL", error_type=type(error).__name__, test_objects_retained=True)
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
        raise

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
