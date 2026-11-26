import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from transformer.common.silver import BATCH_SIZE


METADATA_FIELDS = ("product_name", "brand", "category_id", "category_code", "price")
OBSERVED_AT_FIELDS = {name: f"{name}_observed_at" for name in METADATA_FIELDS}
PRODUCT_DIMENSION_SCHEMA = pa.schema([
    pa.field("source_dataset", pa.string(), nullable=False),
    pa.field("product_id", pa.string(), nullable=False),
    pa.field("product_name", pa.string()),
    pa.field("brand", pa.string()),
    pa.field("category_id", pa.string()),
    pa.field("category_code", pa.string()),
    pa.field("price", pa.decimal128(18, 2)),
    pa.field("product_name_observed_at", pa.timestamp("us", tz="UTC")),
    pa.field("brand_observed_at", pa.timestamp("us", tz="UTC")),
    pa.field("category_id_observed_at", pa.timestamp("us", tz="UTC")),
    pa.field("category_code_observed_at", pa.timestamp("us", tz="UTC")),
    pa.field("price_observed_at", pa.timestamp("us", tz="UTC")),
])


def _metadata_string(value, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string or null")
    return value if value.strip() else None


def _metadata_price(value) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("price must be a finite Decimal or null")
    return value


def _timestamp_text(value) -> str:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("event_time must be a timezone-aware timestamp")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _json_value(value):
    if isinstance(value, (datetime, Decimal)):
        return str(value) if isinstance(value, Decimal) else value.isoformat()
    raise TypeError(f"Unsupported Product Dimension hash value {type(value).__name__}")


def _update_digest(digest, row: dict) -> None:
    values = [row[name] for name in PRODUCT_DIMENSION_SCHEMA.names]
    digest.update(
        json.dumps(
            values,
            ensure_ascii=False,
            separators=(",", ":"),
            default=_json_value,
            allow_nan=False,
        ).encode("utf-8") + b"\n"
    )


def _conflict_count(connection, field_name: str) -> int:
    return connection.execute(
        f"SELECT COUNT(*) FROM ("
        f"SELECT product_id FROM observations "
        f"WHERE {field_name} IS NOT NULL "
        f"GROUP BY product_id HAVING COUNT(DISTINCT {field_name}) > 1)"
    ).fetchone()[0]


def write_product_dimension(
    source: Path,
    destination: Path,
    source_dataset: str,
    *,
    product_name_column: str | None = None,
) -> dict:
    if not isinstance(source_dataset, str) or not source_dataset:
        raise ValueError("source_dataset must be a nonempty string")
    source_schema = pq.read_schema(source)
    required = {"event_time", "product_id", "brand", "category_id", "category_code", "price"}
    if product_name_column is not None:
        required.add(product_name_column)
    missing = required.difference(source_schema.names)
    if missing:
        raise ValueError(f"Missing Silver columns: {', '.join(sorted(missing))}")
    if not pa.types.is_timestamp(source_schema.field("event_time").type):
        raise ValueError("Silver event_time must be a timestamp")
    if not pa.types.is_decimal(source_schema.field("price").type):
        raise ValueError("Silver price must be a decimal")

    input_columns = ["event_time", "product_id", "brand", "category_id", "category_code", "price"]
    if product_name_column is not None:
        input_columns.append(product_name_column)
    input_columns = list(dict.fromkeys(input_columns))

    source_rows = 0
    with TemporaryDirectory(prefix="nutee-product-dimension-") as directory:
        database = Path(directory) / "products.sqlite"
        connection = sqlite3.connect(database)
        try:
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute(
                "CREATE TABLE observations ("
                "product_id TEXT NOT NULL, event_time TEXT NOT NULL, "
                "product_name TEXT, brand TEXT, category_id TEXT, category_code TEXT, price TEXT)"
            )
            with pq.ParquetFile(source) as parquet:
                for batch in parquet.iter_batches(batch_size=BATCH_SIZE, columns=input_columns):
                    observations = []
                    for row in batch.to_pylist():
                        product_id = row["product_id"]
                        if not isinstance(product_id, str) or not product_id.strip():
                            raise ValueError("Silver product_id must be a nonempty string")
                        product_name = (
                            _metadata_string(row[product_name_column], "product_name")
                            if product_name_column is not None else None
                        )
                        price = _metadata_price(row["price"])
                        observations.append((
                            product_id,
                            _timestamp_text(row["event_time"]),
                            product_name,
                            _metadata_string(row["brand"], "brand"),
                            _metadata_string(row["category_id"], "category_id"),
                            _metadata_string(row["category_code"], "category_code"),
                            format(price, "f") if price is not None else None,
                        ))
                    connection.executemany(
                        "INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?)",
                        observations,
                    )
                    source_rows += len(observations)

            if not source_rows:
                raise ValueError("No Silver product observations found")
            connection.execute("CREATE INDEX observations_product_time ON observations(product_id, event_time)")
            connection.execute(
                "CREATE TABLE products (product_id TEXT PRIMARY KEY)"
            )
            connection.execute("INSERT INTO products SELECT DISTINCT product_id FROM observations")
            product_count = connection.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            if not product_count:
                raise ValueError("No valid Silver product IDs found")

            for field_name in METADATA_FIELDS:
                latest_table = f"latest_{field_name}"
                connection.execute(
                    f"CREATE TABLE {latest_table} AS "
                    f"SELECT observed.product_id, MIN(observed.{field_name}) AS value, observed.event_time "
                    f"FROM observations AS observed "
                    f"JOIN (SELECT product_id, MAX(event_time) AS event_time FROM observations "
                    f"WHERE {field_name} IS NOT NULL GROUP BY product_id) AS latest "
                    f"ON observed.product_id = latest.product_id "
                    f"AND observed.event_time = latest.event_time "
                    f"WHERE observed.{field_name} IS NOT NULL "
                    f"GROUP BY observed.product_id, observed.event_time"
                )
                connection.execute(
                    f"CREATE UNIQUE INDEX {latest_table}_product ON {latest_table}(product_id)"
                )

            missing_counts = {
                field_name: product_count - connection.execute(
                    f"SELECT COUNT(*) FROM latest_{field_name}"
                ).fetchone()[0]
                for field_name in METADATA_FIELDS
            }
            metadata_conflicts = {
                field_name: _conflict_count(connection, field_name)
                for field_name in METADATA_FIELDS
            }
            conflict_products = connection.execute(
                "SELECT COUNT(DISTINCT product_id) FROM ("
                + " UNION ".join(
                    f"SELECT product_id FROM observations WHERE {field_name} IS NOT NULL "
                    f"GROUP BY product_id HAVING COUNT(DISTINCT {field_name}) > 1"
                    for field_name in METADATA_FIELDS
                )
                + ")"
            ).fetchone()[0]

            untraceable = {}
            for field_name in METADATA_FIELDS:
                untraceable[field_name] = connection.execute(
                    f"SELECT COUNT(*) FROM latest_{field_name} AS selected "
                    f"WHERE NOT EXISTS (SELECT 1 FROM observations AS observed "
                    f"WHERE observed.product_id = selected.product_id "
                    f"AND observed.event_time = selected.event_time "
                    f"AND observed.{field_name} = selected.value)"
                ).fetchone()[0]
            if any(untraceable.values()):
                raise ValueError(f"Selected metadata cannot be traced to Silver: {untraceable}")

            select_columns = ["products.product_id"]
            joins = []
            for field_name in METADATA_FIELDS:
                latest_table = f"latest_{field_name}"
                joins.append(
                    f"LEFT JOIN {latest_table} ON products.product_id = {latest_table}.product_id"
                )
                select_columns.extend((
                    f"{latest_table}.value AS {field_name}",
                    f"{latest_table}.event_time AS {OBSERVED_AT_FIELDS[field_name]}",
                ))
            query = (
                f"SELECT {', '.join(select_columns)} FROM products "
                f"{' '.join(joins)} ORDER BY products.product_id"
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            digest = sha256()
            with pq.ParquetWriter(
                destination,
                PRODUCT_DIMENSION_SCHEMA,
                compression="snappy",
            ) as writer:
                cursor = connection.execute(query)
                while True:
                    records = cursor.fetchmany(BATCH_SIZE)
                    if not records:
                        break
                    output_rows = []
                    for record in records:
                        row = {
                            "source_dataset": source_dataset,
                            "product_id": record[0],
                        }
                        for index, field_name in enumerate(METADATA_FIELDS):
                            value = record[1 + index * 2]
                            observed_at = record[2 + index * 2]
                            row[field_name] = Decimal(value) if field_name == "price" and value is not None else value
                            row[OBSERVED_AT_FIELDS[field_name]] = (
                                datetime.fromisoformat(observed_at) if observed_at is not None else None
                            )
                        output_rows.append(row)
                        _update_digest(digest, row)
                    writer.write_table(
                        pa.Table.from_pylist(output_rows, schema=PRODUCT_DIMENSION_SCHEMA)
                    )
        finally:
            connection.close()

    return {
        "source_rows": source_rows,
        "product_count": product_count,
        "missing_counts": missing_counts,
        "missing_rates": {
            field_name: missing_counts[field_name] / product_count
            for field_name in METADATA_FIELDS
        },
        "metadata_conflicts": metadata_conflicts,
        "products_with_metadata_conflicts": conflict_products,
        "untraceable_metadata_values": untraceable,
        "content_sha256": digest.hexdigest(),
    }


def validate_product_dimension(path: Path, source_dataset: str) -> dict:
    count = 0
    missing_counts = {name: 0 for name in METADATA_FIELDS}
    previous_product_id = None
    digest = sha256()
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(PRODUCT_DIMENSION_SCHEMA, check_metadata=False):
            raise ValueError("Product Dimension schema does not match the contract")
        for group in range(parquet.metadata.num_row_groups):
            for column in range(len(PRODUCT_DIMENSION_SCHEMA)):
                if parquet.metadata.row_group(group).column(column).compression != "SNAPPY":
                    raise ValueError("Product Dimension must use Snappy compression")
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            batch.validate(full=True)
            for row in batch.to_pylist():
                if row["source_dataset"] != source_dataset:
                    raise ValueError("source_dataset does not match the configured dataset")
                product_id = row["product_id"]
                if not isinstance(product_id, str) or not product_id.strip():
                    raise ValueError("Product Dimension product_id must be nonempty")
                if previous_product_id is not None and product_id <= previous_product_id:
                    raise ValueError("Product Dimension product_id values must be unique and sorted")
                previous_product_id = product_id
                for field_name in METADATA_FIELDS:
                    value = row[field_name]
                    observed_at = row[OBSERVED_AT_FIELDS[field_name]]
                    if value is None:
                        missing_counts[field_name] += 1
                        if observed_at is not None:
                            raise ValueError(f"{OBSERVED_AT_FIELDS[field_name]} requires a metadata value")
                    elif observed_at is None:
                        raise ValueError(f"{field_name} requires its Silver observation timestamp")
                    elif field_name != "price" and (
                        not isinstance(value, str) or not value.strip()
                    ):
                        raise ValueError(f"{field_name} must be a nonempty string or null")
                    elif field_name == "price" and (
                        not isinstance(value, Decimal) or not value.is_finite()
                    ):
                        raise ValueError("price must be a finite Decimal or null")
                _update_digest(digest, row)
                count += 1
    if not count:
        raise ValueError("No Product Dimension rows found")
    return {
        "product_count": count,
        "missing_counts": missing_counts,
        "missing_rates": {name: missing_counts[name] / count for name in METADATA_FIELDS},
        "content_sha256": digest.hexdigest(),
    }
