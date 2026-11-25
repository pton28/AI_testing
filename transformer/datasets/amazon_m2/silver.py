"""Normalize Amazon-M2 without inventing event types or consuming target labels."""

import ast
import hashlib
import json
import re

import pyarrow as pa


def product_key(locale: str, asin: str) -> str:
    if locale not in {"UK", "DE", "JP", "ES", "FR", "IT"}:
        raise ValueError(f"Invalid locale {locale!r}")
    if not isinstance(asin, str) or not re.fullmatch(r"[A-Z0-9]{10}", asin):
        raise ValueError(f"Invalid ASIN {asin!r}")
    return f"{locale}:{asin}"


def parse_items(value: str) -> list[str]:
    # Source exports may use NumPy-style ['ASIN1' 'ASIN2'] without commas.
    if not isinstance(value, str) or not re.fullmatch(r"\s*\[\s*(?:(?:'[^']*'|\"[^\"]*\")[\s,]*)*\]\s*", value):
        raise ValueError("prev_items must be a bracketed list of quoted ASINs")
    items = [ast.literal_eval(match.group()) for match in re.finditer(r"'[^']*'|\"[^\"]*\"", value)]
    if not items:
        raise ValueError("prev_items must not be empty")
    return items


class AmazonM2Sessions:
    required_columns = {"locale", "prev_items", "next_item"}
    silver_schema = pa.schema([
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("locale", pa.string(), nullable=False),
        pa.field("prev_items", pa.list_(pa.string()), nullable=False),
        pa.field("next_item", pa.string(), nullable=False),
    ])

    def map_source_row(self, row: dict, source_version: str | None, row_number: int) -> dict:
        if not source_version or not re.fullmatch(r"[a-f0-9]{64}", source_version):
            raise ValueError("Session IDs require the source SHA-256")
        identity = f"amazon_m2:sessions:{source_version}:{row_number}"
        return {"session_id": hashlib.sha256(identity.encode("utf-8")).hexdigest(),
                **self.map_event(row)}

    def map_event(self, row: dict) -> dict:
        locale = (row["locale"] or "").strip()
        return {
            "locale": locale,
            "prev_items": [product_key(locale, asin) for asin in parse_items(row["prev_items"])],
            "next_item": product_key(locale, (row["next_item"] or "").strip()),
        }


class AmazonM2Products:
    required_columns = {"locale", "id", "title"}
    silver_schema = pa.schema([
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("locale", pa.string(), nullable=False),
        pa.field("title", pa.string()),
        pa.field("desc", pa.string()),
        pa.field("brand", pa.string()),
        pa.field("color", pa.string()),
        pa.field("price", pa.string()),
        pa.field("source_attributes", pa.string(), nullable=False),
    ])

    def map_event(self, row: dict) -> dict:
        locale = (row["locale"] or "").strip()
        return {"product_id": product_key(locale, (row["id"] or "").strip()),
                "locale": locale, "title": row["title"] or None,
                **{key: row.get(key) or None for key in ("desc", "brand", "color", "price")},
                "source_attributes": json.dumps(row, ensure_ascii=False)}
