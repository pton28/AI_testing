"""Source-independent Gold event contract, writer and readback validation."""

import json
import math
import tempfile
from collections import Counter
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from transformer.common.silver import BATCH_SIZE


GOLD_SCHEMA_V1 = pa.schema([
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
GOLD_SCHEMA = GOLD_SCHEMA_V1.append(pa.field("session_key", pa.string()))
GOLD_SCHEMA_V3 = GOLD_SCHEMA.append(pa.field("order_ambiguous", pa.bool_()))
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


def build_session_key(source_dataset: str, user_id: str | None, session_id: str | None) -> str | None:
    if not isinstance(source_dataset, str) or not source_dataset:
        raise ValueError("source_dataset must be a nonempty string")
    if user_id is not None and not isinstance(user_id, str):
        raise TypeError("user_id must be a string or None")
    if session_id is not None and not isinstance(session_id, str):
        raise TypeError("session_id must be a string or None")
    if user_id is None or user_id == "" or session_id is None or session_id == "":
        return None
    identity = json.dumps(
        [source_dataset, user_id, session_id],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"nutee-session-v1:{sha256(identity.encode('utf-8')).hexdigest()}"


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
    if "session_key" in event:
        expected = build_session_key(event["source_dataset"], event.get("user_id"), event.get("session_id"))
        if event["session_key"] != expected:
            raise ValueError("session_key does not match source_dataset, user_id and session_id")
    if "order_ambiguous" in event:
        ambiguous = event["order_ambiguous"]
        position = event.get("position_in_session")
        if event.get("session_key") is None:
            if position is not None or ambiguous is not None:
                raise ValueError("Null session_key requires null position and order_ambiguous")
        elif (
            isinstance(position, bool)
            or not isinstance(position, int)
            or position < 1
            or not isinstance(ambiguous, bool)
        ):
            raise ValueError("Gold V3 sessions require positive positions and boolean order_ambiguous")


class _EventStats:
    def __init__(self, source_dataset: str, schema=GOLD_SCHEMA):
        self.source_dataset = source_dataset
        self.schema = schema
        self.ids = set()
        self.counts = Counter()
        self.nulls = Counter({name: 0 for name in schema.names})
        self.digest = sha256()

    def add(self, event: dict):
        validate_event(event)
        if event["source_dataset"] != self.source_dataset:
            raise ValueError("source_dataset does not match the configured source")
        if event["event_id"] in self.ids:
            raise ValueError(f"Duplicate event_id {event['event_id']!r}")
        self.ids.add(event["event_id"])
        self.counts[event["event_type"]] += 1
        self.nulls.update(name for name in self.schema.names if event[name] is None)
        self.digest.update(json.dumps(event, sort_keys=True, default=str, ensure_ascii=False).encode("utf-8") + b"\n")

    def result(self):
        return {"rows": len(self.ids), "event_counts": dict(self.counts),
                "null_counts": dict(self.nulls), "content_sha256": self.digest.hexdigest()}


def validate_gold(path: Path, source_dataset: str, schema=GOLD_SCHEMA) -> dict:
    supported_schemas = (GOLD_SCHEMA_V1, GOLD_SCHEMA, GOLD_SCHEMA_V3)
    if not any(schema.equals(item, check_metadata=False) for item in supported_schemas):
        raise ValueError("Unsupported Gold schema version")
    stats = _EventStats(source_dataset, schema)
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(schema, check_metadata=False):
            version = (
                "V1" if schema.equals(GOLD_SCHEMA_V1, check_metadata=False)
                else "V2" if schema.equals(GOLD_SCHEMA, check_metadata=False)
                else "V3"
            )
            raise ValueError(f"Gold schema does not match the {version} contract")
        for group in range(parquet.metadata.num_row_groups):
            for column in range(len(schema)):
                if parquet.metadata.row_group(group).column(column).compression != "SNAPPY":
                    raise ValueError("Gold must use Snappy compression")
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            batch.validate(full=True)
            for event in batch.to_pylist():
                stats.add(event)
    if not stats.ids:
        raise ValueError("No Gold events found")
    return stats.result()


def order_gold_v3(source: Path, destination: Path, source_dataset: str) -> dict:
    table = pq.read_table(source)
    if not table.schema.equals(GOLD_SCHEMA, check_metadata=False):
        raise ValueError("Gold V3 ordering requires a Gold V2 input")

    indices = pc.sort_indices(
        table,
        sort_keys=[
            ("session_key", "ascending"),
            ("event_time", "ascending"),
            ("event_id", "ascending"),
        ],
        null_placement="at_end",
    )
    ordered = pc.take(table, indices)
    session_keys = ordered["session_key"].to_pylist()
    event_times = ordered["event_time"].to_pylist()
    positions = [None] * ordered.num_rows
    ambiguous = [None] * ordered.num_rows
    current_session = None
    position = 0

    for index, (session_key, event_time) in enumerate(zip(session_keys, event_times)):
        if session_key is None:
            continue
        if session_key != current_session:
            current_session = session_key
            position = 1
        else:
            position += 1
        positions[index] = position
        ambiguous[index] = False
        if (
            event_time is not None
            and index > 0
            and session_keys[index - 1] == session_key
            and event_times[index - 1] == event_time
        ):
            ambiguous[index - 1] = True
            ambiguous[index] = True

    position_index = ordered.schema.get_field_index("position_in_session")
    ordered = ordered.set_column(
        position_index,
        GOLD_SCHEMA.field("position_in_session"),
        pa.array(positions, type=pa.int32()),
    )
    ordered = ordered.append_column(
        GOLD_SCHEMA_V3.field("order_ambiguous"),
        pa.array(ambiguous, type=pa.bool_()),
    )
    with pq.ParquetWriter(destination, GOLD_SCHEMA_V3, compression="snappy") as writer:
        for offset in range(0, ordered.num_rows, BATCH_SIZE):
            writer.write_table(ordered.slice(offset, BATCH_SIZE))
    return validate_gold(destination, source_dataset, schema=GOLD_SCHEMA_V3)


def write_gold(
    source: Path,
    destination: Path,
    adapter,
    source_dataset: str,
    source_version: str,
    schema=GOLD_SCHEMA,
) -> dict:
    if not source_dataset.strip() or not source_version.strip():
        raise ValueError("source_dataset and source_version are required")
    if schema.equals(GOLD_SCHEMA_V3, check_metadata=False):
        with tempfile.TemporaryDirectory(prefix="nutee-gold-v2-") as directory:
            gold_v2 = Path(directory) / "events.parquet"
            write_gold(source, gold_v2, adapter, source_dataset, source_version)
            return order_gold_v3(gold_v2, destination, source_dataset)
    if not schema.equals(GOLD_SCHEMA, check_metadata=False):
        raise ValueError("Gold writer only supports V2 and V3 output")
    stats = _EventStats(source_dataset)
    source_row_index = 0
    gold_row_index = 0
    with pq.ParquetFile(source) as silver:
        missing = adapter.required_columns.difference(silver.schema_arrow.names)
        if missing:
            raise ValueError(f"Missing Silver columns: {', '.join(sorted(missing))}")
        with pq.ParquetWriter(destination, GOLD_SCHEMA, compression="snappy") as writer:
            for batch in silver.iter_batches(batch_size=BATCH_SIZE):
                events = []
                for row in batch.to_pylist():
                    current_source_row = source_row_index
                    source_row_index += 1
                    try:
                        event = adapter.map_event(row)
                        if event is None:
                            continue
                        event["source_dataset"] = source_dataset
                        event["session_key"] = build_session_key(
                            source_dataset, event.get("user_id"), event.get("session_id")
                        )
                        # Only absent IDs are synthesized; malformed existing IDs fail validation.
                        event["event_id"] = row.get("event_id")
                        if event["event_id"] is None:
                            event["event_id"] = stable_event_id(source_dataset, source_version, gold_row_index)
                        validate_event(event)
                    except (ValueError, TypeError) as error:
                        raise ValueError(f"Invalid Silver row {current_source_row}: {error}") from error
                    events.append(event)
                    gold_row_index += 1
                if events:
                    table = pa.Table.from_pylist(events, schema=GOLD_SCHEMA)
                    table.validate(full=True)
                    for event in table.to_pylist():
                        stats.add(event)
                    writer.write_table(table)
        if source_row_index != silver.metadata.num_rows:
            raise ValueError("Not all Silver rows were processed")
    if not gold_row_index:
        raise ValueError("No Silver events found")
    return stats.result()
