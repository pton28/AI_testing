import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import ANY, patch

import pyarrow as pa
import pyarrow.parquet as pq

from loader.common.local_store import LocalLoader
from pipeline import build_product_dimension, run
from transformer.common.product_dimension import (
    PRODUCT_DIMENSION_SCHEMA,
    validate_product_dimension,
    write_product_dimension,
)
from transformer.common.silver import SILVER_SCHEMA


VERSION = "a" * 64
MERREC_SILVER_SCHEMA = pa.schema(list(SILVER_SCHEMA) + [pa.field("name", pa.string())])
TIME_1 = datetime(2023, 5, 1, tzinfo=timezone.utc)
TIME_2 = datetime(2023, 5, 2, tzinfo=timezone.utc)


def silver_row(product_id, event_time, **changes):
    row = dict.fromkeys(MERREC_SILVER_SCHEMA.names)
    row.update(
        event_time=event_time,
        event_type="product_view",
        user_id="u1",
        product_id=product_id,
        name=None,
        brand=None,
        category_id=None,
        category_code=None,
        price=None,
    )
    row.update(changes)
    return row


class ProductDimensionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "silver.parquet"
        self.first = self.root / "products-first.parquet"
        self.second = self.root / "products-second.parquet"

    def write_source(self, rows):
        pq.write_table(pa.Table.from_pylist(rows, schema=MERREC_SILVER_SCHEMA), self.source)

    def test_latest_valid_fields_conflicts_provenance_and_determinism(self):
        rows = [
            silver_row(
                "p1", TIME_1, name="Older name", brand="Old brand",
                category_id="1", category_code="Old category", price=Decimal("10.00"),
            ),
            silver_row("p1", TIME_2, brand="Z brand", category_code="New category",
                       price=Decimal("12.00")),
            silver_row("p1", TIME_2, brand="A brand", category_id="2",
                       category_code="New category", price=Decimal("12.00")),
            silver_row("p2", TIME_2, brand="   "),
        ]
        self.write_source(rows)
        first_stats = write_product_dimension(
            self.source, self.first, "merrec", product_name_column="name"
        )
        self.write_source(list(reversed(rows)))
        second_stats = write_product_dimension(
            self.source, self.second, "merrec", product_name_column="name"
        )

        first = pq.read_table(self.first)
        second = pq.read_table(self.second)
        self.assertEqual(first.schema, PRODUCT_DIMENSION_SCHEMA)
        self.assertTrue(first.equals(second))
        self.assertEqual(first_stats, second_stats)
        self.assertEqual(first_stats["source_rows"], 4)
        self.assertEqual(first_stats["product_count"], 2)
        self.assertEqual(first_stats["metadata_conflicts"], {
            "product_name": 0,
            "brand": 1,
            "category_id": 1,
            "category_code": 1,
            "price": 1,
        })
        self.assertEqual(first_stats["products_with_metadata_conflicts"], 1)
        self.assertEqual(first_stats["untraceable_metadata_values"], {
            field: 0 for field in ("product_name", "brand", "category_id", "category_code", "price")
        })
        self.assertEqual(first_stats["missing_counts"]["product_name"], 1)

        p1, p2 = first.to_pylist()
        self.assertEqual(p1["product_id"], "p1")
        self.assertEqual(p1["product_name"], "Older name")
        self.assertEqual(p1["product_name_observed_at"], TIME_1)
        self.assertEqual(p1["brand"], "A brand")
        self.assertEqual(p1["brand_observed_at"], TIME_2)
        self.assertEqual(p1["category_id"], "2")
        self.assertEqual(p1["category_code"], "New category")
        self.assertEqual(p1["price"], Decimal("12.00"))
        self.assertEqual(p1["price_observed_at"], TIME_2)
        self.assertEqual(p2["product_id"], "p2")
        self.assertIsNone(p2["product_name"])
        self.assertIsNone(p2["brand"])
        self.assertEqual(
            validate_product_dimension(self.first, "merrec")["content_sha256"],
            first_stats["content_sha256"],
        )

    def test_missing_product_name_column_and_invalid_product_id(self):
        rows = [silver_row("p1", TIME_1, brand="Brand")]
        self.write_source(rows)
        stats = write_product_dimension(self.source, self.first, "rees46_electronics")
        self.assertEqual(stats["product_count"], 1)
        self.assertEqual(stats["missing_rates"]["product_name"], 1.0)

        self.write_source([silver_row("  ", TIME_1)])
        with self.assertRaisesRegex(ValueError, "product_id"):
            write_product_dimension(self.source, self.first, "merrec", product_name_column="name")

    def test_pipeline_builds_product_dimension_from_silver_and_supports_rerun(self):
        source = self.root / "merrec-silver.parquet"
        pq.write_table(
            pa.Table.from_pylist([silver_row("p1", TIME_1, name="Title")], schema=MERREC_SILVER_SCHEMA),
            source,
        )
        loader = LocalLoader(self.root / "data")
        silver_key = f"silver/merrec/merrec_events/{VERSION}/events.parquet"
        loader.load(silver_key, source, "application/vnd.apache.parquet")
        dataset = {"name": "merrec", "adapter": "merrec_events"}

        first_key, first_stats = build_product_dimension(loader, dataset, VERSION)
        second_key, second_stats = build_product_dimension(loader, dataset, VERSION)

        self.assertEqual(first_key, f"gold/v3/merrec/{VERSION}/products.parquet")
        self.assertEqual(first_stats, second_stats)
        self.assertEqual(pq.read_table(loader.path(first_key))["product_name"].to_pylist(), ["Title"])

    def test_product_dimension_only_cli_routes_to_existing_silver(self):
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
                        "--product-dimension-only", "--source-version", VERSION,
                    ]), \
                    patch("pipeline.build_product_dimension") as build, \
                    patch("pipeline.extract", side_effect=AssertionError("Must not re-extract source")):
                run()
            build.assert_called_once_with(ANY, dataset, VERSION)


if __name__ == "__main__":
    unittest.main()
