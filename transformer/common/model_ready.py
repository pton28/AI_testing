"""Build model-ready interaction and session aggregates from Gold V3."""

import json
import sqlite3
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from transformer.common.gold import GOLD_SCHEMA_V3
from transformer.common.product_dimension import validate_product_dimension
from transformer.common.silver import BATCH_SIZE


ALS_INTERACTIONS_SCHEMA = pa.schema([
    pa.field("user_id", pa.string(), nullable=False),
    pa.field("product_id", pa.string(), nullable=False),
    pa.field("view_count", pa.int64(), nullable=False),
    pa.field("cart_count", pa.int64(), nullable=False),
    pa.field("purchase_count", pa.int64(), nullable=False),
    pa.field("interaction_count", pa.int64(), nullable=False),
    pa.field("last_interaction_at", pa.timestamp("us", tz="UTC")),
])
SESSION_SEQUENCES_SCHEMA = pa.schema([
    pa.field("session_key", pa.string(), nullable=False),
    pa.field("user_id", pa.string(), nullable=False),
    pa.field("item_sequence", pa.list_(pa.string()), nullable=False),
    pa.field("event_sequence", pa.list_(pa.string()), nullable=False),
    pa.field("timestamp_sequence", pa.list_(pa.timestamp("us", tz="UTC")), nullable=False),
    pa.field("sequence_length", pa.int32(), nullable=False),
    pa.field("has_ambiguous_order", pa.bool_(), nullable=False),
])

EVENT_COUNT_FIELDS = {
    "product_view": "view_count",
    "add_to_cart": "cart_count",
    "purchase": "purchase_count",
}
_GOLD_COLUMNS = [
    "source_dataset", "event_type", "event_time", "user_id", "session_key",
    "position_in_session", "product_id", "order_ambiguous",
]


def _timestamp_text(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("event_time must be a timezone-aware timestamp or null")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse_timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _digest_row(digest, row: dict) -> None:
    digest.update(
        json.dumps(
            row,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=lambda value: value.isoformat() if isinstance(value, datetime) else value,
            allow_nan=False,
        ).encode("utf-8") + b"\n"
    )


def _write_als(connection: sqlite3.Connection, destination: Path) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = sha256()
    pairs = 0
    totals = {field: 0 for field in EVENT_COUNT_FIELDS.values()}
    total_interactions = 0
    with pq.ParquetWriter(
        destination, ALS_INTERACTIONS_SCHEMA, compression="snappy"
    ) as writer:
        cursor = connection.execute(
            "SELECT user_id, product_id, view_count, cart_count, purchase_count, "
            "last_interaction_at FROM interactions ORDER BY user_id, product_id"
        )
        while True:
            records = cursor.fetchmany(BATCH_SIZE)
            if not records:
                break
            rows = []
            for user_id, product_id, views, carts, purchases, last_at in records:
                interaction_count = views + carts + purchases
                row = {
                    "user_id": user_id,
                    "product_id": product_id,
                    "view_count": views,
                    "cart_count": carts,
                    "purchase_count": purchases,
                    "interaction_count": interaction_count,
                    "last_interaction_at": _parse_timestamp(last_at),
                }
                _digest_row(digest, row)
                rows.append(row)
                totals["view_count"] += views
                totals["cart_count"] += carts
                totals["purchase_count"] += purchases
                total_interactions += interaction_count
                pairs += 1
            writer.write_table(pa.Table.from_pylist(rows, schema=ALS_INTERACTIONS_SCHEMA))
    if not pairs:
        raise ValueError("No valid user-product interactions found")
    return {
        "pair_count": pairs,
        "view_count": totals["view_count"],
        "cart_count": totals["cart_count"],
        "purchase_count": totals["purchase_count"],
        "interaction_count": total_interactions,
        "content_sha256": digest.hexdigest(),
    }


def _write_sequences(connection: sqlite3.Connection, destination: Path) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = sha256()
    sessions = 0
    events = 0
    ambiguous_sessions = 0
    ambiguous_events = 0
    pending_rows = []
    with pq.ParquetWriter(
        destination, SESSION_SEQUENCES_SCHEMA, compression="snappy"
    ) as writer:
        cursor = connection.execute(
            "SELECT session_key, position, user_id, product_id, event_type, "
            "event_time, order_ambiguous FROM sequence_events "
            "ORDER BY session_key, position"
        )
        current_key = None
        current_user = None
        items = []
        event_types = []
        timestamps = []
        ambiguous = False

        def flush():
            nonlocal sessions, events, ambiguous_sessions, ambiguous_events
            if current_key is None:
                return None
            length = len(items)
            if not length or length > 2**31 - 1:
                raise ValueError("Session sequence length is outside the supported integer range")
            row = {
                "session_key": current_key,
                "user_id": current_user,
                "item_sequence": list(items),
                "event_sequence": list(event_types),
                "timestamp_sequence": list(timestamps),
                "sequence_length": length,
                "has_ambiguous_order": ambiguous,
            }
            _digest_row(digest, row)
            pending_rows.append(row)
            if len(pending_rows) >= BATCH_SIZE:
                writer.write_table(
                    pa.Table.from_pylist(pending_rows, schema=SESSION_SEQUENCES_SCHEMA)
                )
                pending_rows.clear()
            sessions += 1
            events += length
            if ambiguous:
                ambiguous_sessions += 1
                ambiguous_events += sum(
                    1 for value in current_ambiguities if value
                )
            return row

        current_ambiguities = []
        for session_key, position, user_id, product_id, event_type, event_time, ambiguous_order in cursor:
            if session_key != current_key:
                flush()
                current_key = session_key
                current_user = user_id
                items = []
                event_types = []
                timestamps = []
                current_ambiguities = []
                ambiguous = False
            if current_user != user_id:
                raise ValueError(f"session_key {session_key!r} maps to multiple users")
            if position != len(items) + 1:
                raise ValueError(f"session_key {session_key!r} has non-contiguous positions")
            items.append(product_id)
            event_types.append(event_type)
            timestamps.append(_parse_timestamp(event_time))
            current_ambiguities.append(bool(ambiguous_order))
            ambiguous = ambiguous or bool(ambiguous_order)
        flush()
        if pending_rows:
            writer.write_table(
                pa.Table.from_pylist(pending_rows, schema=SESSION_SEQUENCES_SCHEMA)
            )
    if not sessions:
        raise ValueError("No non-null session_key sequences found")
    return {
        "sequence_count": sessions,
        "sequence_event_count": events,
        "ambiguous_session_count": ambiguous_sessions,
        "ambiguous_event_count": ambiguous_events,
        "content_sha256": digest.hexdigest(),
    }


def write_model_ready(
    gold_source: Path,
    products_source: Path,
    als_destination: Path,
    sequences_destination: Path,
    source_dataset: str,
) -> dict:
    if not isinstance(source_dataset, str) or not source_dataset:
        raise ValueError("source_dataset must be a nonempty string")
    if not pq.read_schema(gold_source).equals(GOLD_SCHEMA_V3, check_metadata=False):
        raise ValueError("Model-ready aggregation requires Gold V3")
    product_stats = validate_product_dimension(products_source, source_dataset)

    with TemporaryDirectory(prefix="nutee-model-ready-") as directory:
        connection = sqlite3.connect(Path(directory) / "model-ready.sqlite")
        try:
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute(
                "CREATE TABLE interactions ("
                "user_id TEXT NOT NULL, product_id TEXT NOT NULL, "
                "view_count INTEGER NOT NULL DEFAULT 0, "
                "cart_count INTEGER NOT NULL DEFAULT 0, "
                "purchase_count INTEGER NOT NULL DEFAULT 0, "
                "last_interaction_at TEXT, PRIMARY KEY(user_id, product_id))"
            )
            connection.execute(
                "CREATE TABLE sequence_events ("
                "session_key TEXT NOT NULL, position INTEGER NOT NULL, "
                "user_id TEXT NOT NULL, product_id TEXT NOT NULL, "
                "event_type TEXT NOT NULL, event_time TEXT, "
                "order_ambiguous INTEGER NOT NULL, "
                "PRIMARY KEY(session_key, position))"
            )
            connection.execute(
                "CREATE TABLE product_ids (product_id TEXT PRIMARY KEY)"
            )
            with pq.ParquetFile(products_source) as products:
                for batch in products.iter_batches(batch_size=BATCH_SIZE, columns=["product_id"]):
                    product_ids = batch.column(0).to_pylist()
                    connection.executemany(
                        "INSERT INTO product_ids VALUES (?)",
                        [(product_id,) for product_id in product_ids],
                    )

            source_rows = 0
            event_counts = {kind: 0 for kind in EVENT_COUNT_FIELDS}
            null_session_events = 0
            count_field_indexes = {
                name: index for index, name in enumerate(
                    ("view_count", "cart_count", "purchase_count")
                )
            }
            with pq.ParquetFile(gold_source) as gold:
                for batch in gold.iter_batches(batch_size=BATCH_SIZE, columns=_GOLD_COLUMNS):
                    pair_counts = {}
                    sequence_rows = []
                    for row in batch.to_pylist():
                        source_rows += 1
                        if row["source_dataset"] != source_dataset:
                            raise ValueError("Gold source_dataset does not match the configured dataset")
                        event_type = row["event_type"]
                        if event_type not in EVENT_COUNT_FIELDS:
                            raise ValueError(f"Unsupported model-ready event_type {event_type!r}")
                        event_counts[event_type] += 1
                        user_id = row["user_id"]
                        product_id = row["product_id"]
                        if not isinstance(user_id, str) or not user_id.strip():
                            raise ValueError("Gold user_id must be nonempty for model-ready interactions")
                        if not isinstance(product_id, str) or not product_id.strip():
                            raise ValueError("Gold product_id must be nonempty for model-ready interactions")
                        event_time = _timestamp_text(row["event_time"])
                        pair = (user_id, product_id)
                        counts = pair_counts.setdefault(
                            pair, [0, 0, 0, None]
                        )
                        count_field = EVENT_COUNT_FIELDS[event_type]
                        counts[count_field_indexes[count_field]] += 1
                        if event_time is not None and (
                            counts[3] is None or event_time > counts[3]
                        ):
                            counts[3] = event_time

                        session_key = row["session_key"]
                        position = row["position_in_session"]
                        ambiguous = row["order_ambiguous"]
                        if session_key is None:
                            null_session_events += 1
                            if position is not None or ambiguous is not None:
                                raise ValueError(
                                    "Gold null session_key requires null position and ambiguity"
                                )
                            continue
                        if not isinstance(session_key, str) or not session_key:
                            raise ValueError("Gold session_key must be nonempty or null")
                        if isinstance(position, bool) or not isinstance(position, int) or position < 1:
                            raise ValueError("Gold session events require a positive integer position")
                        if not isinstance(ambiguous, bool):
                            raise ValueError("Gold session events require boolean order_ambiguous")
                        sequence_rows.append((
                            session_key,
                            position,
                            user_id,
                            product_id,
                            event_type,
                            event_time,
                            int(ambiguous),
                        ))
                    connection.executemany(
                        "INSERT INTO interactions VALUES (?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(user_id, product_id) DO UPDATE SET "
                        "view_count = view_count + excluded.view_count, "
                        "cart_count = cart_count + excluded.cart_count, "
                        "purchase_count = purchase_count + excluded.purchase_count, "
                        "last_interaction_at = CASE "
                        "WHEN excluded.last_interaction_at IS NULL THEN interactions.last_interaction_at "
                        "WHEN interactions.last_interaction_at IS NULL "
                        "OR excluded.last_interaction_at > interactions.last_interaction_at "
                        "THEN excluded.last_interaction_at ELSE interactions.last_interaction_at END",
                        [
                            (user, product, counts[0], counts[1], counts[2], counts[3])
                            for (user, product), counts in pair_counts.items()
                        ],
                    )
                    connection.executemany(
                        "INSERT INTO sequence_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                        sequence_rows,
                    )
            if not source_rows:
                raise ValueError("No Gold V3 events found")
            coverage = connection.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT product_id FROM interactions) AS used "
                "LEFT JOIN product_ids USING (product_id) WHERE product_ids.product_id IS NULL"
            ).fetchone()[0]
            if coverage:
                raise ValueError(f"{coverage} interaction products are absent from Product Dimension")
            if connection.execute(
                "SELECT COUNT(*) FROM (SELECT session_key FROM sequence_events "
                "GROUP BY session_key HAVING COUNT(DISTINCT user_id) > 1)"
            ).fetchone()[0]:
                raise ValueError("A session_key maps to multiple users")

            als_stats = _write_als(connection, als_destination)
            sequence_stats = _write_sequences(connection, sequences_destination)
        finally:
            connection.close()

    return {
        "gold_event_count": source_rows,
        "gold_event_counts": event_counts,
        "null_session_events": null_session_events,
        "product_dimension_count": product_stats["product_count"],
        "product_coverage_missing": coverage,
        "als_interactions": als_stats,
        "session_sequences": sequence_stats,
    }


def validate_als_interactions(path: Path) -> dict:
    pairs = 0
    totals = {field: 0 for field in ("view_count", "cart_count", "purchase_count")}
    total_interactions = 0
    previous_pair = None
    digest = sha256()
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(ALS_INTERACTIONS_SCHEMA, check_metadata=False):
            raise ValueError("ALS interactions schema does not match the contract")
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            for row in batch.to_pylist():
                user_id = row["user_id"]
                product_id = row["product_id"]
                if not isinstance(user_id, str) or not user_id.strip():
                    raise ValueError("ALS user_id must be nonempty")
                if not isinstance(product_id, str) or not product_id.strip():
                    raise ValueError("ALS product_id must be nonempty")
                pair = (user_id, product_id)
                if previous_pair is not None and pair <= previous_pair:
                    raise ValueError("ALS user-product pairs must be unique and sorted")
                previous_pair = pair
                counts = [row[name] for name in ("view_count", "cart_count", "purchase_count")]
                if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts):
                    raise ValueError("ALS event counts must be nonnegative integers")
                if row["interaction_count"] != sum(counts) or row["interaction_count"] <= 0:
                    raise ValueError("interaction_count must equal the three event counts")
                last_at = row["last_interaction_at"]
                if last_at is not None and (
                    not isinstance(last_at, datetime) or last_at.utcoffset() is None
                ):
                    raise ValueError("last_interaction_at must be a timezone-aware timestamp or null")
                _digest_row(digest, row)
                for name, value in zip(totals, counts):
                    totals[name] += value
                total_interactions += row["interaction_count"]
                pairs += 1
    if not pairs:
        raise ValueError("No ALS interaction records found")
    return {
        "pair_count": pairs,
        "duplicate_pairs": 0,
        "null_user_ids": 0,
        "null_product_ids": 0,
        **totals,
        "interaction_count": total_interactions,
        "content_sha256": digest.hexdigest(),
    }


def validate_session_sequences(path: Path) -> dict:
    sessions = 0
    total_length = 0
    ambiguous_sessions = 0
    ambiguous_events = 0
    previous_key = None
    digest = sha256()
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(SESSION_SEQUENCES_SCHEMA, check_metadata=False):
            raise ValueError("Session sequences schema does not match the contract")
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            for row in batch.to_pylist():
                key = row["session_key"]
                user_id = row["user_id"]
                items = row["item_sequence"]
                event_types = row["event_sequence"]
                timestamps = row["timestamp_sequence"]
                length = row["sequence_length"]
                ambiguous = row["has_ambiguous_order"]
                if not isinstance(key, str) or not key or not isinstance(user_id, str) or not user_id.strip():
                    raise ValueError("Session sequence key and user_id must be nonempty")
                if previous_key is not None and key <= previous_key:
                    raise ValueError("Session sequence keys must be unique and sorted")
                previous_key = key
                if isinstance(length, bool) or not isinstance(length, int) or length < 1:
                    raise ValueError("Session sequence length must be a positive integer")
                if not (len(items) == len(event_types) == len(timestamps) == length):
                    raise ValueError("Session sequence arrays must have the declared equal length")
                if any(not isinstance(item, str) or not item.strip() for item in items):
                    raise ValueError("Session item_sequence must contain nonempty product IDs")
                if any(event_type not in EVENT_COUNT_FIELDS for event_type in event_types):
                    raise ValueError("Session event_sequence contains an unsupported event type")
                if any(
                    timestamp is not None
                    and (not isinstance(timestamp, datetime) or timestamp.utcoffset() is None)
                    for timestamp in timestamps
                ):
                    raise ValueError("Session timestamps must be timezone-aware or null")
                if not isinstance(ambiguous, bool):
                    raise ValueError("has_ambiguous_order must be boolean")
                _digest_row(digest, row)
                if ambiguous:
                    ambiguous_sessions += 1
                sessions += 1
                total_length += length
    if not sessions:
        raise ValueError("No session sequences found")
    return {
        "sequence_count": sessions,
        "sequence_event_count": total_length,
        "duplicate_session_keys": 0,
        "ambiguous_session_count": ambiguous_sessions,
        "content_sha256": digest.hexdigest(),
    }
