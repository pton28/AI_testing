from decimal import Decimal, InvalidOperation

from transformer.common.silver import parse_timestamp


class Rees46Events:
    required_columns = {"event_time", "event_type", "user_id", "product_id"}

    event_types = {
        "view": "product_view",
        "cart": "add_to_cart",
        "purchase": "purchase",
    }

    def map_event(self, row: dict[str, str | None]) -> dict | None:
        source_event = (row.get("event_type") or "").strip()
        if source_event == "remove_from_cart":
            return None
        if source_event not in self.event_types:
            raise ValueError(f"Unsupported event_type {source_event!r}")

        user_id = (row.get("user_id") or "").strip()
        product_id = (row.get("product_id") or "").strip()
        if not user_id or not product_id:
            raise ValueError("user_id and product_id are required")

        raw_price = (row.get("price") or "").strip()
        try:
            price = Decimal(raw_price) if raw_price else None
        except InvalidOperation as error:
            raise ValueError(f"Invalid price {raw_price!r}") from error
        if price is not None and not price.is_finite():
            raise ValueError(f"Invalid price {raw_price!r}")

        return {
            "event_time": parse_timestamp((row.get("event_time") or "").strip()),
            "event_type": self.event_types[source_event],
            "user_id": user_id,
            "product_id": product_id,
            "session_id": row.get("user_session") or None,
            "category_id": row.get("category_id") or None,
            "category_code": row.get("category_code") or None,
            "brand": row.get("brand") or None,
            "price": price,
        }
