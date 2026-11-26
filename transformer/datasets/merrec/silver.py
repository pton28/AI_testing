"""MerRec Parquet mapping; source semantics: arxiv.org/html/2402.14230v2."""

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import pyarrow as pa

from transformer.common.silver import SILVER_SCHEMA


class MerrecEvents:
    required_columns = {"user_id", "stime", "session_id", "event_id", "item_id", "product_id", "name"}
    event_types = {"item_view": "product_view", "item_add_to_cart_tap": "add_to_cart", "buy_comp": "purchase"}
    excluded_events = {"item_like", "offer_make", "buy_start"}
    silver_schema = pa.schema(list(SILVER_SCHEMA) + [
        pa.field("source_event_type", pa.string()),
        pa.field("source_product_id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("sequence_id", pa.string()),
        pa.field("sequence_length", pa.int64()),
        pa.field("source_attributes", pa.string()),
    ])

    def map_event(self, row: dict) -> dict:
        kind = row["event_id"]  # This source field is an action name, NOT a unique ID.
        normalized_event_type = self.event_types.get(kind)
        if normalized_event_type is None and kind not in self.excluded_events:
            raise ValueError(f"Unsupported MerRec event {kind!r}")
        user = self.identifier(row["user_id"])
        product = self.identifier(row["item_id"])
        if user is None or product is None:
            raise ValueError("user_id and item_id are required")
        timestamp = row["stime"]
        if not isinstance(timestamp, datetime):
            raise ValueError("stime must be a timestamp")
        # Paper table 1 explicitly defines stime as UTC; Parquet omits tz metadata.
        timestamp = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc)
        price = row.get("price")
        try:
            price = Decimal(str(price)) if price is not None else None
            if price is not None and (not price.is_finite() or price != price.quantize(Decimal("0.01"))):
                raise ValueError(f"Invalid price {price!r}")
        except InvalidOperation as error:
            raise ValueError(f"Invalid price {price!r}") from error
        return {
            "event_time": timestamp,
            "event_type": normalized_event_type or kind,
            "user_id": user,
            "product_id": product,
            "session_id": self.identifier(row["session_id"]),
            "category_id": self.identifier(row.get("c2_id")),
            "category_code": row.get("c2_name"),
            "brand": row.get("brand_name"),
            "price": price,
            "source_event_type": kind,
            "source_product_id": row["product_id"],
            "name": row["name"],
            "sequence_id": row.get("sequence_id"),
            "sequence_length": row.get("sequence_length"),
            # Preserve all original product attributes without expanding Gold.
            "source_attributes": json.dumps(row, ensure_ascii=False, default=str),
        }

    @staticmethod
    def identifier(value):
        return str(value).strip() or None if value is not None else None
