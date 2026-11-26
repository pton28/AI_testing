"""Register dataset adapters here; shared transformers do not import them."""

from transformer.datasets.rees46_electronics.silver import Rees46Events
from transformer.datasets.rees46_electronics.gold import Rees46GoldEvents
from transformer.datasets.merrec.silver import MerrecEvents
from transformer.datasets.merrec.gold import MerrecGoldEvents
from transformer.datasets.amazon_m2.silver import AmazonM2Sessions, AmazonM2Products
from transformer.datasets.coveo_sigir_ecom_2021.silver import (
    CoveoBrowsingEvents, CoveoSearchEvents, CoveoSkuContent,
)


ADAPTERS = {"rees46_events": Rees46Events(), "merrec_events": MerrecEvents()}
GOLD_ADAPTERS = {"rees46_events": Rees46GoldEvents(), "merrec_events": MerrecGoldEvents()}
PRODUCT_NAME_COLUMNS = {"rees46_events": None, "merrec_events": "name"}

SILVER_ASSET_ADAPTERS = {
    "amazon_m2_sessions": AmazonM2Sessions(),
    "amazon_m2_products": AmazonM2Products(),
    "coveo_browsing_events": CoveoBrowsingEvents(),
    "coveo_search_events": CoveoSearchEvents(),
    "coveo_sku_content": CoveoSkuContent(),
}
