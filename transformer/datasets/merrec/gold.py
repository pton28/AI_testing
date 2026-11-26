from transformer.common.gold import NormalizedGoldEvents
from transformer.datasets.merrec.silver import MerrecEvents


class MerrecGoldEvents(NormalizedGoldEvents):
    """Listing-level MerRec: no synthesized actions or session positions."""
    event_types = {"product_view", "add_to_cart", "purchase"}

    def map_event(self, row: dict) -> dict | None:
        if row.get("source_event_type") in MerrecEvents.excluded_events:
            return None
        return super().map_event(row)
