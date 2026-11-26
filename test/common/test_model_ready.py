import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import ANY, patch

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import run
from transformer.common.gold import GOLD_SCHEMA_V3
from transformer.common.model_ready import (
    ALS_INTERACTIONS_SCHEMA,
    SESSION_SEQUENCES_SCHEMA,
    validate_als_interactions,
    validate_session_sequences,
    write_model_ready,
)
from transformer.common.product_dimension import PRODUCT_DIMENSION_SCHEMA


VERSION = "a" * 64
TIME_1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
TIME_2 = datetime(2025, 1, 2, tzinfo=timezone.utc)
TIME_3 = datetime(2025, 1, 3, tzinfo=timezone.utc)
TIME_4 = datetime(2025, 1, 4, tzinfo=timezone.utc)


def gold_row(event_id, event_type, user_id, product_id, session_key, position, event_time, ambiguous):
    row = dict.fromkeys(GOLD_SCHEMA_V3.names)
    row.update(
        event_id=event_id,
        source_dataset="merrec",
        event_type=event_type,
        event_time=event_time,
        user_id=user_id,
        session_id=None,
        position_in_session=position,
        product_id=product_id,
        query_text=None,
        rating_value=None,
        event_origin="observed",
        session_key=session_key,
        order_ambiguous=ambiguous,
    )
    return row


class ModelReadyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.gold = self.root / "gold.parquet"
        self.shuffled_gold = self.root / "gold-shuffled.parquet"
        self.products = self.root / "products.parquet"
        self.als = self.root / "als.parquet"
        self.sequences = self.root / "sequences.parquet"
        self.als_shuffled = self.root / "als-shuffled.parquet"
        self.sequences_shuffled = self.root / "sequences-shuffled.parquet"
        self.key_1 = "session-key-1"
        self.key_2 = "session-key-2"
        self.rows = [
            gold_row("e3", "add_to_cart", "u1", "p2", self.key_1, 3, TIME_2, False),
            gold_row("e1", "product_view", "u1", "p1", self.key_1, 1, TIME_1, True),
            gold_row("e4", "purchase", "u1", "p1", self.key_2, 1, TIME_3, False),
            gold_row("e-null", "product_view", "u2", "p2", None, None, TIME_4, None),
            gold_row("e2", "product_view", "u1", "p1", self.key_1, 2, TIME_1, True),
        ]
        pq.write_table(pa.Table.from_pylist(self.rows, schema=GOLD_SCHEMA_V3), self.gold)
        product_rows = []
        for product_id in ("p1", "p2"):
            row = dict.fromkeys(PRODUCT_DIMENSION_SCHEMA.names)
            row.update(source_dataset="merrec", product_id=product_id)
            product_rows.append(row)
        pq.write_table(
            pa.Table.from_pylist(product_rows, schema=PRODUCT_DIMENSION_SCHEMA),
            self.products,
        )

    def test_aggregates_preserve_duplicates_null_sessions_and_sequence_order(self):
        stats = write_model_ready(
            self.gold, self.products, self.als, self.sequences, "merrec"
        )
        self.assertEqual(pq.read_schema(self.als), ALS_INTERACTIONS_SCHEMA)
        self.assertEqual(pq.read_schema(self.sequences), SESSION_SEQUENCES_SCHEMA)
        als_rows = pq.read_table(self.als).to_pylist()
        self.assertEqual(len(als_rows), 3)
        first_pair = next(row for row in als_rows if row["user_id"] == "u1" and row["product_id"] == "p1")
        self.assertEqual(
            (first_pair["view_count"], first_pair["cart_count"], first_pair["purchase_count"],
             first_pair["interaction_count"]),
            (2, 0, 1, 3),
        )
        self.assertEqual(first_pair["last_interaction_at"], TIME_3)
        self.assertEqual(stats["null_session_events"], 1)
        self.assertEqual(stats["als_interactions"]["interaction_count"], 5)
        self.assertEqual(stats["als_interactions"]["view_count"], 3)
        self.assertEqual(stats["als_interactions"]["cart_count"], 1)
        self.assertEqual(stats["als_interactions"]["purchase_count"], 1)
        self.assertEqual(stats["session_sequences"]["sequence_count"], 2)
        self.assertEqual(stats["session_sequences"]["sequence_event_count"], 4)
        self.assertEqual(stats["session_sequences"]["ambiguous_session_count"], 1)
        self.assertEqual(stats["session_sequences"]["ambiguous_event_count"], 2)

        sequences = pq.read_table(self.sequences).to_pylist()
        first_session = next(row for row in sequences if row["session_key"] == self.key_1)
        self.assertEqual(first_session["user_id"], "u1")
        self.assertEqual(first_session["item_sequence"], ["p1", "p1", "p2"])
        self.assertEqual(
            first_session["event_sequence"],
            ["product_view", "product_view", "add_to_cart"],
        )
        self.assertEqual(first_session["timestamp_sequence"], [TIME_1, TIME_1, TIME_2])
        self.assertTrue(first_session["has_ambiguous_order"])
        self.assertEqual(
            next(row for row in sequences if row["session_key"] == self.key_2)["sequence_length"],
            1,
        )
        self.assertEqual(validate_als_interactions(self.als)["interaction_count"], 5)
        self.assertEqual(validate_session_sequences(self.sequences)["sequence_event_count"], 4)

    def test_output_is_deterministic_when_gold_input_rows_are_reordered(self):
        pq.write_table(
            pa.Table.from_pylist(list(reversed(self.rows)), schema=GOLD_SCHEMA_V3),
            self.shuffled_gold,
        )
        first = write_model_ready(
            self.gold, self.products, self.als, self.sequences, "merrec"
        )
        second = write_model_ready(
            self.shuffled_gold,
            self.products,
            self.als_shuffled,
            self.sequences_shuffled,
            "merrec",
        )
        self.assertTrue(pq.read_table(self.als).equals(pq.read_table(self.als_shuffled)))
        self.assertTrue(
            pq.read_table(self.sequences).equals(pq.read_table(self.sequences_shuffled))
        )
        self.assertEqual(first, second)

    def test_rejects_missing_product_dimension_coverage(self):
        rows = [dict.fromkeys(PRODUCT_DIMENSION_SCHEMA.names)]
        rows[0].update(source_dataset="merrec", product_id="p1")
        pq.write_table(
            pa.Table.from_pylist(rows, schema=PRODUCT_DIMENSION_SCHEMA), self.products
        )
        with self.assertRaisesRegex(ValueError, "absent from Product Dimension"):
            write_model_ready(
                self.gold, self.products, self.als, self.sequences, "merrec"
            )

    def test_model_ready_only_cli_uses_existing_gold_and_products(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "data").mkdir()
            dataset = {"name": "merrec", "adapter": "merrec_events"}
            (root / "data/datasets.json").write_text(
                json.dumps({"datasets": [dataset]}), encoding="utf-8"
            )
            with patch("pipeline.__file__", str(root / "pipeline.py")), \
                    patch("sys.argv", [
                        "run_pipeline.py", "--dataset", "merrec", "--storage", "local",
                        "--model-ready-only", "--source-version", VERSION,
                    ]), \
                    patch("pipeline.build_model_ready") as build, \
                    patch("pipeline.extract", side_effect=AssertionError("Must not re-extract source")):
                run()
            build.assert_called_once_with(ANY, dataset, VERSION)


if __name__ == "__main__":
    unittest.main()
