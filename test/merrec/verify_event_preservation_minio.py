"""Verify MerRec event preservation using an isolated MinIO bucket."""

import argparse
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import datetime
from hashlib import sha256
from itertools import zip_longest
from pathlib import Path

import pyarrow.parquet as pq

from loader.common.minio_store import MinioLoader
from pipeline import build_gold, create_loader
from transformer.common.gold import GOLD_SCHEMA, build_session_key, validate_gold
from transformer.common.silver import BATCH_SIZE, write_silver
from transformer.datasets.merrec.silver import MerrecEvents


SOURCE_VERSION = "3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca"
G2_TEST_BUCKET = "nutee-session-v2-test-20261126b"
BRONZE_COUNTS = {
    "item_view": 504756,
    "item_add_to_cart_tap": 10168,
    "buy_comp": 677,
    "item_like": 83571,
    "offer_make": 4260,
    "buy_start": 1837,
}
GOLD_COUNTS = {"product_view": 504756, "add_to_cart": 10168, "purchase": 677}
GOLD_FIELDS = GOLD_SCHEMA.names
REPORT_PATH = Path(__file__).resolve().parents[2] / "reports" / "event_preservation_g3" / "integration.json"


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


def _snapshot_digest(snapshot: dict) -> str:
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _source_digest(row: dict) -> bytes:
    encoded = json.dumps(
        row,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).digest()


def _gold_digest(row: dict) -> bytes:
    encoded = json.dumps(
        [row[name] for name in GOLD_FIELDS],
        separators=(",", ":"),
        ensure_ascii=False,
        default=lambda value: value.isoformat() if isinstance(value, datetime) else str(value),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).digest()


def _add_multiset(connection, table: str, side: str, digests) -> None:
    counts = Counter(digests)
    connection.executemany(
        f"INSERT INTO {table}(side, digest, occurrences) VALUES (?, ?, ?) "
        "ON CONFLICT(side, digest) DO UPDATE SET occurrences = occurrences + excluded.occurrences",
        [(side, digest, count) for digest, count in counts.items()],
    )
    connection.commit()


def _multiset_matches(connection, table: str) -> bool:
    mismatch = connection.execute(
        f"SELECT COUNT(*) FROM {table} AS old "
        "LEFT JOIN (SELECT digest, occurrences FROM " + table + " WHERE side = 'new') AS new USING (digest) "
        "WHERE old.side = 'old' AND (new.digest IS NULL OR old.occurrences != new.occurrences)"
    ).fetchone()[0]
    additional = connection.execute(
        f"SELECT COUNT(*) FROM {table} AS new "
        "LEFT JOIN (SELECT digest, occurrences FROM " + table + " WHERE side = 'old') AS old USING (digest) "
        "WHERE new.side = 'new' AND old.digest IS NULL"
    ).fetchone()[0]
    return mismatch == 0 and additional == 0


def _compare_source_multisets(connection, bronze_path: Path, silver_path: Path) -> tuple[int, int, bool]:
    connection.execute(
        "CREATE TABLE source_rows (side TEXT, digest BLOB, occurrences INTEGER, PRIMARY KEY(side, digest))"
    )
    bronze_rows = 0
    with pq.ParquetFile(bronze_path) as bronze:
        for batch in bronze.iter_batches(batch_size=BATCH_SIZE):
            rows = batch.to_pylist()
            _add_multiset(connection, "source_rows", "old", (_source_digest(row) for row in rows))
            bronze_rows += len(rows)

    silver_rows = 0
    with pq.ParquetFile(silver_path) as silver:
        for batch in silver.iter_batches(batch_size=BATCH_SIZE, columns=["source_attributes"]):
            source_rows = (json.loads(value) for value in batch.column(0).to_pylist())
            _add_multiset(connection, "source_rows", "new", (_source_digest(row) for row in source_rows))
            silver_rows += batch.num_rows
    return bronze_rows, silver_rows, _multiset_matches(connection, "source_rows")


def _compare_gold_multisets(connection, old_path: Path, new_path: Path) -> bool:
    connection.execute(
        "CREATE TABLE gold_rows (side TEXT, digest BLOB, occurrences INTEGER, PRIMARY KEY(side, digest))"
    )
    for side, path in (("old", old_path), ("new", new_path)):
        with pq.ParquetFile(path) as parquet:
            for batch in parquet.iter_batches(batch_size=BATCH_SIZE, columns=GOLD_FIELDS):
                _add_multiset(connection, "gold_rows", side, (_gold_digest(row) for row in batch.to_pylist()))
    return _multiset_matches(connection, "gold_rows")


def _ordered_event_id_changes(old_path: Path, new_path: Path) -> int:
    with pq.ParquetFile(old_path) as old, pq.ParquetFile(new_path) as new:
        old_ids = (
            event_id
            for batch in old.iter_batches(batch_size=BATCH_SIZE, columns=["event_id"])
            for event_id in batch.column(0).to_pylist()
        )
        new_ids = (
            event_id
            for batch in new.iter_batches(batch_size=BATCH_SIZE, columns=["event_id"])
            for event_id in batch.column(0).to_pylist()
        )
        return sum(left != right for left, right in zip_longest(old_ids, new_ids, fillvalue=None))


def _session_profile(path: Path, database: Path) -> dict:
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE sessions (session_key TEXT PRIMARY KEY, source_dataset TEXT, "
        "user_id TEXT, session_id TEXT, UNIQUE(source_dataset, user_id, session_id))"
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
                        raise AssertionError("Gold session_key differs from its source tuple")
                    if expected is None:
                        null_keys += 1
                        continue
                    identity = (row["source_dataset"], row["user_id"], row["session_id"])
                    previous = entries.get(expected)
                    if previous is not None and previous != identity:
                        raise AssertionError("Distinct session tuples share a session_key")
                    entries[expected] = identity
                with connection:
                    for key, identity in entries.items():
                        cursor = connection.execute(
                            "INSERT OR IGNORE INTO sessions VALUES (?, ?, ?, ?)",
                            (key, *identity),
                        )
                        if cursor.rowcount == 0:
                            existing = connection.execute(
                                "SELECT source_dataset, user_id, session_id FROM sessions WHERE session_key = ?",
                                (key,),
                            ).fetchone()
                            if existing != identity:
                                raise AssertionError("Session key collision across source tuples")
                            other_key = connection.execute(
                                "SELECT session_key FROM sessions "
                                "WHERE source_dataset = ? AND user_id = ? AND session_id = ?",
                                identity,
                            ).fetchone()
                            if other_key != (key,):
                                raise AssertionError("One session tuple maps to multiple keys")

        unique_keys = connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        shared_keys = connection.execute(
            "SELECT COUNT(*) FROM (SELECT session_key FROM sessions GROUP BY session_key "
            "HAVING COUNT(DISTINCT user_id) > 1)"
        ).fetchone()[0]
        return {
            "unique_session_keys": unique_keys,
            "null_session_keys": null_keys,
            "shared_session_keys": shared_keys,
        }
    finally:
        connection.close()


def _criteria(expected, actual) -> dict:
    return {
        name: {
            "expected": expected[name],
            "actual": actual[name],
            "status": "PASS" if actual[name] == expected[name] else "FAIL",
        }
        for name in expected
    }


def _inventory_result(client, bucket: str) -> dict:
    snapshot = _snapshot(client, bucket)
    return {"object_count": len(snapshot), "sha256": _snapshot_digest(snapshot)}


def run(test_bucket: str) -> dict:
    if test_bucket in {G2_TEST_BUCKET}:
        raise ValueError("GĐ3 requires a test bucket distinct from the GĐ2 baseline bucket")

    official = create_loader()
    if not official.client.bucket_exists(official.bucket):
        raise RuntimeError("Official MinIO bucket is unavailable; refusing to run")
    if test_bucket == official.bucket or official.client.bucket_exists(test_bucket):
        raise ValueError("The GĐ3 test bucket must be new and distinct from the official bucket")
    if not official.client.bucket_exists(G2_TEST_BUCKET):
        raise RuntimeError("Accepted GĐ2 baseline bucket is unavailable; refusing to run")

    version = SOURCE_VERSION
    bronze_key = (
        f"bronze/merrec/{version}/000000000030.parquet"
    )
    g2_gold_key = f"gold/v2/merrec/{version}/events.parquet"
    rees_gold_key = (
        "gold/v2/rees46_electronics/"
        "cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/events.parquet"
    )
    silver_key = f"silver/merrec/merrec_events/{version}/events.parquet"
    new_gold_key = f"gold/v2/merrec/{version}/events.parquet"

    official_versioning_before = official.client.get_bucket_versioning(official.bucket).status
    official_before = _inventory_result(official.client, official.bucket)
    g2_before = _inventory_result(official.client, G2_TEST_BUCKET)
    for bucket, key in (
        (official.bucket, bronze_key),
        (G2_TEST_BUCKET, g2_gold_key),
        (G2_TEST_BUCKET, rees_gold_key),
    ):
        official.client.stat_object(bucket, key)

    with tempfile.TemporaryDirectory(prefix="nutee-g3-merrec-") as directory:
        temporary = Path(directory)
        bronze_path = temporary / "bronze.parquet"
        silver_path = temporary / "silver.parquet"
        old_gold_path = temporary / "g2-merrec-gold.parquet"
        new_gold_path = temporary / "g3-merrec-gold.parquet"
        rees_gold_path = temporary / "g2-rees-gold.parquet"

        official.client.fget_object(official.bucket, bronze_key, str(bronze_path))
        official.client.fget_object(G2_TEST_BUCKET, g2_gold_key, str(old_gold_path))
        official.client.fget_object(G2_TEST_BUCKET, rees_gold_key, str(rees_gold_path))

        bronze_counts = Counter()
        with pq.ParquetFile(bronze_path) as bronze:
            bronze_rows = bronze.metadata.num_rows
            for batch in bronze.iter_batches(batch_size=BATCH_SIZE, columns=["event_id"]):
                bronze_counts.update(batch.column(0).to_pylist())
        if bronze_rows != 605269 or dict(bronze_counts) != BRONZE_COUNTS:
            raise AssertionError("MerRec Bronze row count or source action distribution differs from expected")

        silver_rows = write_silver(bronze_path, silver_path, MerrecEvents(), source_version=version)
        silver_counts = Counter()
        with pq.ParquetFile(silver_path) as silver:
            for batch in silver.iter_batches(batch_size=BATCH_SIZE, columns=["source_event_type"]):
                silver_counts.update(batch.column(0).to_pylist())
        if silver_rows != bronze_rows or dict(silver_counts) != dict(bronze_counts):
            raise AssertionError("MerRec Silver did not preserve all Bronze source actions")

        comparison = sqlite3.connect(temporary / "comparison.sqlite")
        try:
            bronze_rows_checked, silver_rows_checked, source_multiset_matches = _compare_source_multisets(
                comparison, bronze_path, silver_path
            )
            if (
                bronze_rows_checked != bronze_rows
                or silver_rows_checked != bronze_rows
                or not source_multiset_matches
            ):
                raise AssertionError("Bronze source attributes do not match Silver as a multiset")

            old_gold_stats = validate_gold(old_gold_path, "merrec")
            rees_gold_stats = validate_gold(rees_gold_path, "rees46_electronics")
            if old_gold_stats["rows"] != 515601 or rees_gold_stats["rows"] != 885129:
                raise AssertionError("Accepted GĐ2 Gold V2 baseline counts are unexpected")

            test_store = MinioLoader(official.client, test_bucket)
            test_store.ensure_bucket()
            test_store.load(silver_key, silver_path, "application/vnd.apache.parquet")
            build_gold(test_store, {"name": "merrec", "adapter": "merrec_events"}, version)
            official.client.fget_object(test_bucket, new_gold_key, str(new_gold_path))
            new_gold_stats = validate_gold(new_gold_path, "merrec")

            if new_gold_stats["event_counts"] != GOLD_COUNTS or new_gold_stats["rows"] != 515601:
                raise AssertionError("Gold V2 emitted unexpected event rows or event types")
            gold_event_ids_changed = _ordered_event_id_changes(old_gold_path, new_gold_path)
            if gold_event_ids_changed != 0:
                raise AssertionError("Gold event IDs changed in processing order")
            gold_multiset_matches = _compare_gold_multisets(comparison, old_gold_path, new_gold_path)
            if not gold_multiset_matches:
                raise AssertionError("Gold V2 12-field multiset differs from the accepted GĐ2 baseline")
            session_profile = _session_profile(new_gold_path, temporary / "sessions.sqlite")
            if session_profile != {
                "unique_session_keys": 112859,
                "null_session_keys": 0,
                "shared_session_keys": 0,
            }:
                raise AssertionError("Gold V2 session key acceptance criteria differ from GĐ2")
        finally:
            comparison.close()

    official_after = _inventory_result(official.client, official.bucket)
    g2_after = _inventory_result(official.client, G2_TEST_BUCKET)
    official_versioning_after = official.client.get_bucket_versioning(official.bucket).status
    if official_before != official_after or g2_before != g2_after:
        raise AssertionError("Official or accepted GĐ2 objects changed during the GĐ3 test")
    if official_versioning_before != official_versioning_after:
        raise AssertionError("Official MinIO bucket versioning changed during the GĐ3 test")

    actual = {
        "merrec_bronze_rows": bronze_rows,
        "merrec_silver_rows": silver_rows,
        "restored_silver_events": sum(BRONZE_COUNTS[kind] for kind in MerrecEvents.excluded_events),
        "merrec_gold_v2_rows": new_gold_stats["rows"],
        "product_view": new_gold_stats["event_counts"].get("product_view", 0),
        "add_to_cart": new_gold_stats["event_counts"].get("add_to_cart", 0),
        "purchase": new_gold_stats["event_counts"].get("purchase", 0),
        "unique_gold_session_keys": session_profile["unique_session_keys"],
        "shared_session_keys": session_profile["shared_session_keys"],
        "null_session_keys": session_profile["null_session_keys"],
        "gold_events_lost_or_added": 0 if gold_multiset_matches else -1,
        "gold_event_ids_changed": gold_event_ids_changed,
        "rees46_gold_rows": rees_gold_stats["rows"],
    }
    expected = {
        "merrec_bronze_rows": 605269,
        "merrec_silver_rows": 605269,
        "restored_silver_events": 89668,
        "merrec_gold_v2_rows": 515601,
        "product_view": 504756,
        "add_to_cart": 10168,
        "purchase": 677,
        "unique_gold_session_keys": 112859,
        "shared_session_keys": 0,
        "null_session_keys": 0,
        "gold_events_lost_or_added": 0,
        "gold_event_ids_changed": 0,
        "rees46_gold_rows": 885129,
    }
    report = {
        "status": "PASS",
        "baseline_bucket": G2_TEST_BUCKET,
        "test_bucket": test_bucket,
        "bronze_key": bronze_key,
        "silver_test_key": silver_key,
        "gold_v2_baseline_key": g2_gold_key,
        "gold_v2_test_key": new_gold_key,
        "source_event_distribution": dict(bronze_counts),
        "silver_source_event_distribution": dict(silver_counts),
        "gold_event_distribution": new_gold_stats["event_counts"],
        "bronze_silver_source_attributes_multiset_matches": source_multiset_matches,
        "gold_v2_12_field_multiset_matches": gold_multiset_matches,
        "gold_event_ids_changed_in_processing_order": gold_event_ids_changed,
        "gold_content_sha256_before": old_gold_stats["content_sha256"],
        "gold_content_sha256_after": new_gold_stats["content_sha256"],
        "acceptance_criteria": _criteria(expected, actual),
        "official_inventory_before": official_before,
        "official_inventory_after": official_after,
        "g2_baseline_inventory_before": g2_before,
        "g2_baseline_inventory_after": g2_after,
        "official_bucket_versioning_before": official_versioning_before,
        "official_bucket_versioning_after": official_versioning_after,
        "test_objects_retained": True,
        "gold_rows_before": old_gold_stats["rows"],
        "gold_rows_after": new_gold_stats["rows"],
        "gold_session_profile": session_profile,
    }
    if not all(item["status"] == "PASS" for item in report["acceptance_criteria"].values()):
        raise AssertionError("One or more GĐ3 acceptance criteria failed")
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-bucket", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.test_bucket), indent=2))


if __name__ == "__main__":
    main()
