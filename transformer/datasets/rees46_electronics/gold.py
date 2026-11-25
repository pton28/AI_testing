"""Map normalized REES46 Silver rows to Gold; no inferred events."""


from transformer.common.gold import NormalizedGoldEvents


class Rees46GoldEvents(NormalizedGoldEvents):
    event_types = {"product_view", "add_to_cart", "purchase"}

