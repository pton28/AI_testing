import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from pipeline import run
from transformer.datasets.coveo_sigir_ecom_2021.silver import (
    CoveoBrowsingEvents, CoveoSearchEvents, CoveoSkuContent,
)


class CoveoSilverTests(unittest.TestCase):
    def test_browsing_actions_and_stable_event_identity(self):
        adapter = CoveoBrowsingEvents()
        base = {
            "session_id_hash": "session-hash",
            "event_type": "event_product",
            "product_action": "detail",
            "product_sku_hash": "sku-hash",
            "server_timestamp_epoch_ms": "1550885210881",
            "hashed_url": "url-hash",
        }
        first = adapter.map_source_row(base, "a" * 64, 2)
        self.assertEqual(first["normalized_event_type"], "product_view")
        self.assertEqual(first["event_time"], datetime(2019, 2, 23, 1, 26, 50, 881000, tzinfo=timezone.utc))
        self.assertEqual(first, adapter.map_source_row(base, "a" * 64, 2))
        self.assertNotEqual(first["event_id"], adapter.map_source_row(base, "a" * 64, 3)["event_id"])
        self.assertIsNone(adapter.map_source_row(
            {**base, "product_action": "", "product_sku_hash": ""}, "a" * 64, 4
        )["normalized_event_type"])
        self.assertEqual(adapter.map_source_row(
            {**base, "product_action": "remove"}, "a" * 64, 5
        )["normalized_event_type"], "remove_from_cart")
        with self.assertRaisesRegex(ValueError, "product_sku_hash is required"):
            adapter.map_source_row({**base, "product_sku_hash": ""}, "a" * 64, 6)

    def test_search_and_content_lists_are_typed_and_validated(self):
        search = CoveoSearchEvents().map_source_row({
            "session_id_hash": "session-hash",
            "server_timestamp_epoch_ms": "1548575194779",
            "query_vector": "[0.25, -0.5]",
            "product_skus_hash": "['sku-a', 'sku-b']",
            "clicked_skus_hash": "",
        }, "b" * 64, 2)
        self.assertEqual(search["query_vector"], [0.25, -0.5])
        self.assertEqual(search["product_skus_hash"], ["sku-a", "sku-b"])
        self.assertIsNone(search["clicked_skus_hash"])
        with self.assertRaisesRegex(ValueError, "finite numbers"):
            CoveoSearchEvents().map_source_row({
                "session_id_hash": "s", "server_timestamp_epoch_ms": "1",
                "query_vector": "[NaN]", "product_skus_hash": "[]", "clicked_skus_hash": "",
            }, "b" * 64, 2)

        product = CoveoSkuContent().map_source_row({
            "product_sku_hash": "sku-a", "description_vector": "[1.0, 2.0]",
            "category_hash": "cat/a", "image_vector": "", "price_bucket": "7.0",
        }, None, 2)
        self.assertEqual(product["description_vector"], [1.0, 2.0])
        self.assertIsNone(product["image_vector"])
        self.assertEqual(product["price_bucket"], 7)
        with self.assertRaisesRegex(ValueError, "price_bucket must be an integer"):
            CoveoSkuContent().map_event({
                "product_sku_hash": "sku-a", "description_vector": "",
                "category_hash": "", "image_vector": "", "price_bucket": "7.5",
            })

    def test_local_silver_only_pipeline_processes_all_three_assets_repeatably(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            data = root / "data"
            source_dir = data / "coveo_sigir_ecom_2021"
            source_dir.mkdir(parents=True)
            rows_by_file = {
                "browsing_train.csv": [{
                    "session_id_hash": "session-hash", "event_type": "event_product",
                    "product_action": "detail", "product_sku_hash": "sku-a",
                    "server_timestamp_epoch_ms": "1550885210881", "hashed_url": "url-hash",
                }],
                "search_train.csv": [{
                    "session_id_hash": "session-hash", "query_vector": "[0.1, 0.2]",
                    "clicked_skus_hash": "['sku-a']", "product_skus_hash": "['sku-a', 'sku-b']",
                    "server_timestamp_epoch_ms": "1548575194779",
                }],
                "sku_to_content.csv": [{
                    "product_sku_hash": "sku-a", "description_vector": "[0.1, 0.2]",
                    "category_hash": "cat/a", "image_vector": "[1.0, 2.0]", "price_bucket": "7.0",
                }],
            }
            for filename, rows in rows_by_file.items():
                with (source_dir / filename).open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
                    writer.writeheader()
                    writer.writerows(rows)

            config = {
                "datasets": [],
                "planned_datasets": [{
                    "name": "coveo_sigir_ecom_2021",
                    "files": [
                        {"source": f"coveo_sigir_ecom_2021/{filename}", "adapter": adapter}
                        for filename, adapter in (
                            ("browsing_train.csv", "coveo_browsing_events"),
                            ("search_train.csv", "coveo_search_events"),
                            ("sku_to_content.csv", "coveo_sku_content"),
                        )
                    ],
                }],
            }
            (data / "datasets.json").write_text(json.dumps(config), encoding="utf-8")
            argv = ["run_pipeline.py", "--dataset", "coveo_sigir_ecom_2021",
                    "--storage", "local", "--silver-only"]
            with patch("pipeline.__file__", str(root / "pipeline.py")), patch("sys.argv", argv):
                run()
                before = {str(path): path.read_bytes() for path in data.glob("silver/**/*.parquet")}
                run()
                after = {str(path): path.read_bytes() for path in data.glob("silver/**/*.parquet")}

            self.assertEqual(len(after), 3)
            self.assertEqual(before, after)
            by_adapter = {
                path.parts[-3]: pq.read_table(path)
                for path in data.glob("silver/**/*.parquet")
            }
            self.assertEqual(by_adapter["coveo_browsing_events"].num_rows, 1)
            self.assertEqual(by_adapter["coveo_search_events"]["product_skus_hash"].to_pylist(),
                             [["sku-a", "sku-b"]])
            self.assertEqual(by_adapter["coveo_sku_content"]["price_bucket"].to_pylist(), [7])
