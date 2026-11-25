from transformer.common.gold import NormalizedGoldEvents


class MerrecGoldEvents(NormalizedGoldEvents):
    """Listing-level MerRec: no synthesized actions or session positions."""
    event_types = {"product_view", "add_to_cart", "purchase"}
