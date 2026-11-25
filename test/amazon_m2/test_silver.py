import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from transformer.common.silver import write_silver
from transformer.datasets.amazon_m2.silver import AmazonM2Products, AmazonM2Sessions, parse_items


class AmazonM2Tests(unittest.TestCase):
    def test_order_duplicates_label_and_locales(self):
        adapter = AmazonM2Sessions()
        row = {"locale": "UK", "prev_items": "['B000000001' 'B000000001'\n 'B000000002']", "next_item": "B000000003"}
        mapped = adapter.map_event(row)
        self.assertEqual(mapped["prev_items"], ["UK:B000000001", "UK:B000000001", "UK:B000000002"])
        self.assertNotIn(mapped["next_item"], mapped["prev_items"])
        other = adapter.map_event({**row, "locale": "DE"})
        self.assertNotEqual(other["prev_items"], mapped["prev_items"])
        first = adapter.map_source_row(row, "a" * 64, 2)
        self.assertEqual(first, adapter.map_source_row(row, "a" * 64, 2))
        self.assertNotEqual(first["session_id"], adapter.map_source_row(row, "a" * 64, 3)["session_id"])
        self.assertNotEqual(first["session_id"], adapter.map_source_row(row, "b" * 64, 2)["session_id"])
        with self.assertRaises(ValueError):
            adapter.map_source_row(row, None, 2)
        self.assertEqual(parse_items('["B000000001", "B000000002"]'), ["B000000001", "B000000002"])
        for value in ("[]", "['A'] garbage", "[__import__('os')]", "['A' + 'B']"):
            with self.assertRaises(ValueError):
                adapter.map_event({**row, "prev_items": value})

    def test_csv_to_parquet_and_catalog_key(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "sessions.csv"
            target = Path(folder) / "sessions.parquet"
            with source.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(["locale", "prev_items", "next_item"])
                writer.writerow(["JP", "['B000000001']", "B000000002"])
            self.assertEqual(write_silver(source, target, AmazonM2Sessions(), source_version="a" * 64), 1)
            session = pq.read_table(target).to_pylist()[0]
            self.assertTrue(session["session_id"])
            product = AmazonM2Products().map_event({"locale": "JP", "id": "B000000001", "title": "Title", "desc": "Text"})
            self.assertEqual(session["prev_items"][0], product["product_id"])
            self.assertIn('"desc": "Text"', product["source_attributes"])
            self.assertEqual(product["desc"], "Text")
            self.assertIsNone(product["price"])
            catalog = Path(folder) / "products.csv"
            catalog.write_text('locale,id,title,desc,brand,color,price\nJP,B000000001,Title,Text,Brand,Blue,123.45\n', encoding="utf-8")
            write_silver(catalog, target, AmazonM2Products())
            stored = pq.read_table(target).to_pylist()[0]
            self.assertEqual([stored[k] for k in ("desc", "brand", "color", "price")], ["Text", "Brand", "Blue", "123.45"])

    def test_silver_only_pipeline_rerun(self):
        from pipeline import run
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            data = root / "data"
            data.mkdir()
            (data / "sessions.csv").write_text("locale,prev_items,next_item\nUK,['B000000001'],B000000002\n")
            (data / "products.csv").write_text("locale,id,title\nUK,B000000001,Example\n")
            config = {"datasets": [], "planned_datasets": [{"name": "amazon_m2", "files": [
                {"source": "sessions.csv", "adapter": "amazon_m2_sessions"},
                {"source": "products.csv", "adapter": "amazon_m2_products"}]}]}
            (data / "datasets.json").write_text(json.dumps(config))
            with patch("pipeline.__file__", str(root / "pipeline.py")), patch("pipeline.build_gold") as gold, \
                    patch("sys.argv", ["run_pipeline.py", "--dataset", "amazon_m2", "--storage", "local", "--silver-only"]):
                run()
                before = {str(p): p.read_bytes() for p in data.glob("silver/**/*.parquet")}
                run()
                after = {str(p): p.read_bytes() for p in data.glob("silver/**/*.parquet")}
                self.assertEqual(len(after), 2)
                self.assertEqual(before, after)
                gold.assert_not_called()
