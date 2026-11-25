import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from transformer.common.gold import GOLD_SCHEMA, validate_event, validate_gold, write_gold

VERSION = "a" * 64


class GoldContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / "silver.parquet"
        self.destination = Path(self.temp.name) / "gold.parquet"

    def write_source(self, rows, schema):
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), self.source)

    def test_general_contract_search_rating_click_and_nullable_identity(self):
        event = dict.fromkeys(GOLD_SCHEMA.names)
        event.update(event_id="id", source_dataset="source", event_type="search", event_origin="observed")
        validate_event(event)  # Anonymous ordered source / vector-only search.
        for event_type in ("product_click", "product_view", "add_to_cart", "purchase", "rating"):
            with self.subTest(event_type=event_type):
                candidate = {**event, "event_type": event_type}
                with self.assertRaisesRegex(ValueError, "product_id"):
                    validate_event(candidate)
                candidate["product_id"] = "p1"
                if event_type == "rating":
                    with self.assertRaisesRegex(ValueError, "rating_value"):
                        validate_event(candidate)
                    candidate["rating_value"] = 4.5
                validate_event(candidate)
        for changes in ({"event_origin": "unknown"}, {"event_type": "click"},
                        {"rating_value": float("nan")}, {"event_time": datetime(2020, 1, 1)}):
            with self.assertRaises(ValueError):
                validate_event({**event, **changes})

    def test_common_writer_preserves_click_view_and_nullable_search(self):
        class IdentityMapping:
            required_columns = set(GOLD_SCHEMA.names)

            def map_event(self, row):
                return dict(row)

        rows = []
        for index, kind in enumerate(("product_click", "product_view", "search", "rating")):
            event = dict.fromkeys(GOLD_SCHEMA.names)
            event.update(event_id=str(index), source_dataset="test_source", event_type=kind,
                         event_origin="observed", product_id=None if kind == "search" else "p1",
                         position_in_session=index, rating_value=4.5 if kind == "rating" else None)
            rows.append(event)
        self.write_source(rows, GOLD_SCHEMA)
        stats = write_gold(self.source, self.destination, IdentityMapping(), "test_source", VERSION)
        self.assertEqual(stats["event_counts"], {"product_click": 1, "product_view": 1, "search": 1, "rating": 1})
        self.assertEqual(stats["null_counts"]["event_time"], 4)
        self.assertEqual(stats["null_counts"]["user_id"], 4)
        self.assertEqual(stats, validate_gold(self.destination, "test_source"))
