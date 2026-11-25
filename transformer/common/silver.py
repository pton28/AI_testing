from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from extractor.common.source import read_rows


SILVER_SCHEMA = pa.schema([
    pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("event_type", pa.string(), nullable=False),
    pa.field("user_id", pa.string(), nullable=False),
    pa.field("product_id", pa.string(), nullable=False),
    pa.field("session_id", pa.string()),
    pa.field("category_id", pa.string()),
    pa.field("category_code", pa.string()),
    pa.field("brand", pa.string()),
    pa.field("price", pa.decimal128(18, 2)),
])

BATCH_SIZE = 25_000


def write_silver(source: Path, destination: Path, adapter, *, source_version: str | None = None) -> int:
    """Apply a source mapping and write the common event schema as Parquet."""
    schema = getattr(adapter, "silver_schema", SILVER_SCHEMA)
    with read_rows(source, BATCH_SIZE) as (columns, rows):
        missing = adapter.required_columns.difference(columns)
        if missing:
            raise ValueError(f"Missing source columns: {', '.join(sorted(missing))}")

        batch = []
        written = 0
        with pq.ParquetWriter(destination, schema, compression="snappy") as writer:
            for row_number, row in rows:
                try:
                    if hasattr(adapter, "map_source_row"):
                        event = adapter.map_source_row(row, source_version, row_number)
                    else:
                        event = adapter.map_event(row)
                except ValueError as error:
                    raise ValueError(f"Invalid source event at row {row_number}: {error}") from error

                if event is None:
                    continue
                batch.append(event)
                if len(batch) == BATCH_SIZE:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    written += len(batch)
                    batch.clear()

            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                written += len(batch)

    if not written:
        raise ValueError("No supported events found in the input file")
    return written


def parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace(" UTC", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("event_time must include a time zone")
    return timestamp.astimezone(timezone.utc)
