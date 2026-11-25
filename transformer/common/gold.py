"""Source-independent Gold event contract, writer and readback validation."""

import json
import math
from collections import Counter
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from transformer.common.silver import BATCH_SIZE


GOLD_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("source_dataset", pa.string(), nullable=False),
    pa.field("event_type", pa.string(), nullable=False),
    pa.field("event_time", pa.timestamp("us", tz="UTC")),
    pa.field("user_id", pa.string()),
    pa.field("session_id", pa.string()),
    pa.field("position_in_session", pa.int32()),
    pa.field("product_id", pa.string()),
    pa.field("query_text", pa.string()),
    pa.field("rating_value", pa.float32()),
    pa.field("event_origin", pa.string(), nullable=False),
])
EVENT_TYPES = {"product_click", "product_view", "search", "add_to_cart", "purchase", "rating"}


class NormalizedGoldEvents:
    """Map normalized, timestamped Silver events with stable user identities."""
    required_columns = {"event_time", "event_type", "user_id", "product_id", "session_id"}
    event_types = EVENT_TYPES

    def map_event(self, row: dict) -> dict:
        if row["event_type"] not in self.event_types:
            raise ValueError(f"Unsupported Silver event_type {row['event_type']!r}")
        if row["event_time"] is None or not row["user_id"]:
            raise ValueError("Silver requires event_time and user_id")
        return {
            "event_type": row["event_type"], "event_time": row["event_time"],
            "user_id": row["user_id"], "session_id": row["session_id"],
            "position_in_session": row.get("position_in_session"),
            "product_id": row["product_id"], "query_text": row.get("query_text"),
            "rating_value": row.get("rating_value"), "event_origin": "observed",
        }


def stable_event_id(dataset: str, source_version: str, row_index: int) -> str:
    identity = json.dumps([dataset, source_version, row_index], ensure_ascii=False, separators=(",", ":"))
    return sha256(identity.encode("utf-8")).hexdigest()


def validate_event(event: dict) -> None:
    for name in ("event_id", "source_dataset", "event_type", "event_origin"):
        if not isinstance(event.get(name), str) or not event[name].strip():
            raise ValueError(f"{name} must be a nonempty string")
    if event["event_type"] not in EVENT_TYPES:
        raise ValueError(f"Unsupported Gold event_type {event['event_type']!r}")
    if event["event_origin"] not in {"observed", "inferred"}:
        raise ValueError("event_origin must be observed or inferred")
    product = event.get("product_id")
    if event["event_type"] != "search" and (not isinstance(product, str) or not product.strip()):
        raise ValueError("product_id is required except for search")
    rating = event.get("rating_value")
    if event["event_type"] == "rating" and rating is None:
        raise ValueError("rating_value is required for rating")
    if rating is not None and (isinstance(rating, bool) or not math.isfinite(rating)):
        raise ValueError("rating_value must be finite")
    timestamp = event.get("event_time")
    if timestamp is not None and (not isinstance(timestamp, datetime) or timestamp.utcoffset() is None):
        raise ValueError("event_time must include a time zone")


class _EventStats:
    def __init__(self, source_dataset: str):
        self.source_dataset = source_dataset
        self.ids = set()
        self.counts = Counter()
        self.nulls = Counter({name: 0 for name in GOLD_SCHEMA.names})
        self.digest = sha256()

    def add(self, event: dict):
        validate_event(event)
        if event["source_dataset"] != self.source_dataset:
            raise ValueError("source_dataset does not match the configured source")
        if event["event_id"] in self.ids:
            raise ValueError(f"Duplicate event_id {event['event_id']!r}")
        self.ids.add(event["event_id"])
        self.counts[event["event_type"]] += 1
        self.nulls.update(name for name in GOLD_SCHEMA.names if event[name] is None)
        self.digest.update(json.dumps(event, sort_keys=True, default=str, ensure_ascii=False).encode("utf-8") + b"\n")

    def result(self):
        return {"rows": len(self.ids), "event_counts": dict(self.counts),
                "null_counts": dict(self.nulls), "content_sha256": self.digest.hexdigest()}


def validate_gold(path: Path, source_dataset: str) -> dict:
    stats = _EventStats(source_dataset)
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(GOLD_SCHEMA, check_metadata=False):
            raise ValueError("Gold schema does not match the 11-column contract")
        for group in range(parquet.metadata.num_row_groups):
            for column in range(len(GOLD_SCHEMA)):
                if parquet.metadata.row_group(group).column(column).compression != "SNAPPY":
                    raise ValueError("Gold must use Snappy compression")
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            batch.validate(full=True)
            for event in batch.to_pylist():
                stats.add(event)
    if not stats.ids:
        raise ValueError("No Gold events found")
    return stats.result()


def write_gold(source: Path, destination: Path, adapter, source_dataset: str, source_version: str) -> dict:
    if not source_dataset.strip() or not source_version.strip():
        raise ValueError("source_dataset and source_version are required")
    stats = _EventStats(source_dataset)
    row_index = 0
    with pq.ParquetFile(source) as silver:
        missing = adapter.required_columns.difference(silver.schema_arrow.names)
        if missing:
            raise ValueError(f"Missing Silver columns: {', '.join(sorted(missing))}")
        with pq.ParquetWriter(destination, GOLD_SCHEMA, compression="snappy") as writer:
            for batch in silver.iter_batches(batch_size=BATCH_SIZE):
                events = []
                for row in batch.to_pylist():
                    try:
                        event = adapter.map_event(row)
                        event["source_dataset"] = source_dataset
                        # Only absent IDs are synthesized; malformed existing IDs fail validation.
                        event["event_id"] = row.get("event_id")
                        if event["event_id"] is None:
                            event["event_id"] = stable_event_id(source_dataset, source_version, row_index)
                        validate_event(event)
                    except (ValueError, TypeError) as error:
                        raise ValueError(f"Invalid Silver row {row_index}: {error}") from error
                    events.append(event)
                    row_index += 1
                table = pa.Table.from_pylist(events, schema=GOLD_SCHEMA)
                table.validate(full=True)
                for event in table.to_pylist():
                    stats.add(event)
                writer.write_table(table)
        if row_index != silver.metadata.num_rows:
            raise ValueError("Gold row count differs from Silver")
    if not row_index:
        raise ValueError("No Silver events found")
    return stats.result()
