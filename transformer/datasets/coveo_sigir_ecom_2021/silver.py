"""Normalize Coveo SIGIR eCom assets without inferring missing identities or actions."""

import ast
import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone

import pyarrow as pa


def _parse_list(value: str | None, field: str) -> list | None:
    if value is None or not value.strip():
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(value)
        except (ValueError, SyntaxError) as error:
            raise ValueError(f"{field} must be a serialized list") from error
    if not isinstance(parsed, list):
        raise ValueError(f"{field} must be a serialized list")
    return parsed


def _parse_vector(value: str | None, field: str) -> list[float] | None:
    values = _parse_list(value, field)
    if values is None:
        return None
    vector = []
    for item in values:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ValueError(f"{field} must contain only finite float32 values")
        try:
            number = float(item)
        except OverflowError as error:
            raise ValueError(f"{field} must contain only finite float32 values") from error
        if not math.isfinite(number) or abs(number) > 3.402823466e38:
            raise ValueError(f"{field} must contain only finite float32 values")
        vector.append(number)
    return vector


def _parse_skus(value: str | None, field: str) -> list[str] | None:
    values = _parse_list(value, field)
    if values is None:
        return None
    if any(not isinstance(item, str) or not item.strip() for item in values):
        raise ValueError(f"{field} must contain nonempty SKU strings")
    return [item.strip() for item in values]


def _event_time(value: str) -> datetime:
    try:
        milliseconds = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("server_timestamp_epoch_ms must be an integer") from error
    if milliseconds < 0:
        raise ValueError("server_timestamp_epoch_ms must be nonnegative")
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=milliseconds)
    except OverflowError as error:
        raise ValueError("server_timestamp_epoch_ms is outside the supported datetime range") from error


def _event_id(asset: str, source_version: str | None, row_number: int) -> str:
    if not source_version or not re.fullmatch(r"[a-f0-9]{64}", source_version):
        raise ValueError("Event IDs require the source SHA-256")
    identity = json.dumps(
        ["coveo_sigir_ecom_2021", asset, source_version, row_number],
        separators=(",", ":"),
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


class CoveoBrowsingEvents:
    required_columns = {
        "session_id_hash", "event_type", "product_action", "product_sku_hash",
        "server_timestamp_epoch_ms",
    }
    actions = {
        "detail": "product_view",
        "add": "add_to_cart",
        "purchase": "purchase",
        "remove": "remove_from_cart",
    }
    silver_schema = pa.schema([
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("product_action", pa.string()),
        pa.field("normalized_event_type", pa.string()),
        pa.field("product_id", pa.string()),
        pa.field("hashed_url", pa.string()),
    ])

    def map_source_row(self, row: dict, source_version: str | None, row_number: int) -> dict:
        session_id = (row["session_id_hash"] or "").strip()
        source_event_type = (row["event_type"] or "").strip()
        action = (row["product_action"] or "").strip() or None
        product_id = (row["product_sku_hash"] or "").strip() or None
        if not session_id or not source_event_type:
            raise ValueError("session_id_hash and event_type are required")
        if action is not None and action not in self.actions:
            raise ValueError(f"Unsupported product_action {action!r}")
        if action is not None and not product_id:
            raise ValueError(f"product_sku_hash is required for product_action {action!r}")
        return {
            "event_id": _event_id("browsing", source_version, row_number),
            "source_row_number": row_number,
            "session_id": session_id,
            "event_time": _event_time(row["server_timestamp_epoch_ms"]),
            "event_type": source_event_type,
            "product_action": action,
            "normalized_event_type": self.actions.get(action),
            "product_id": product_id,
            "hashed_url": (row.get("hashed_url") or "").strip() or None,
        }

    def map_event(self, row: dict) -> dict:
        return self.map_source_row(row, None, 0)


class CoveoSearchEvents:
    required_columns = {
        "session_id_hash", "server_timestamp_epoch_ms", "query_vector",
        "product_skus_hash", "clicked_skus_hash",
    }
    silver_schema = pa.schema([
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("query_vector", pa.list_(pa.float32())),
        pa.field("product_skus_hash", pa.list_(pa.string())),
        pa.field("clicked_skus_hash", pa.list_(pa.string())),
    ])

    def map_source_row(self, row: dict, source_version: str | None, row_number: int) -> dict:
        session_id = (row["session_id_hash"] or "").strip()
        if not session_id:
            raise ValueError("session_id_hash is required")
        return {
            "event_id": _event_id("search", source_version, row_number),
            "source_row_number": row_number,
            "session_id": session_id,
            "event_time": _event_time(row["server_timestamp_epoch_ms"]),
            "query_vector": _parse_vector(row["query_vector"], "query_vector"),
            "product_skus_hash": _parse_skus(row["product_skus_hash"], "product_skus_hash"),
            "clicked_skus_hash": _parse_skus(row["clicked_skus_hash"], "clicked_skus_hash"),
        }

    def map_event(self, row: dict) -> dict:
        return self.map_source_row(row, None, 0)


class CoveoSkuContent:
    required_columns = {
        "product_sku_hash", "description_vector", "category_hash", "image_vector", "price_bucket",
    }
    silver_schema = pa.schema([
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("description_vector", pa.list_(pa.float32())),
        pa.field("category_hash", pa.string()),
        pa.field("image_vector", pa.list_(pa.float32())),
        pa.field("price_bucket", pa.int32()),
    ])

    def map_source_row(self, row: dict, source_version: str | None, row_number: int) -> dict:
        del source_version
        product_id = (row["product_sku_hash"] or "").strip()
        if not product_id:
            raise ValueError("product_sku_hash is required")
        raw_price_bucket = (row["price_bucket"] or "").strip()
        try:
            numeric_bucket = float(raw_price_bucket) if raw_price_bucket else None
        except ValueError as error:
            raise ValueError("price_bucket must be an integer") from error
        if numeric_bucket is not None and not numeric_bucket.is_integer():
            raise ValueError("price_bucket must be an integer")
        price_bucket = int(numeric_bucket) if numeric_bucket is not None else None
        if price_bucket is not None and not -(2 ** 31) <= price_bucket < 2 ** 31:
            raise ValueError("price_bucket is outside the int32 range")
        return {
            "source_row_number": row_number,
            "product_id": product_id,
            "description_vector": _parse_vector(row["description_vector"], "description_vector"),
            "category_hash": (row["category_hash"] or "").strip() or None,
            "image_vector": _parse_vector(row["image_vector"], "image_vector"),
            "price_bucket": price_bucket,
        }

    def map_event(self, row: dict) -> dict:
        return self.map_source_row(row, None, 0)
