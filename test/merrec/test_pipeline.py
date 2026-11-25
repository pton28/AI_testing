import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from loader.common.local_store import LocalLoader
from pipeline import build_gold
from transformer.common.silver import write_silver
from transformer.common.gold import GOLD_SCHEMA, validate_gold
from transformer.datasets.merrec.silver import MerrecEvents


def source_row(kind="item_view", **changes):
    row = dict(user_id=123, stime=datetime(2023, 5, 1), session_id="session-1",
               event_id=kind, item_id=456, product_id="brand_category", name="Example title",
               price=12.34, c2_id=7, c2_name="Shoes", brand_name="Brand",
               sequence_id="20230501_0", sequence_length=22, color="blue")
    row.update(changes)
    return row


class MerrecTests(unittest.TestCase):
    def test_mapping_semantics_and_metadata(self):
        adapter = MerrecEvents()
        for raw, normalized in adapter.event_types.items():
            row = adapter.map_event(source_row(raw, session_id=None))
            self.assertEqual(row["event_type"], normalized)
            self.assertEqual(row["product_id"], "456")
            self.assertEqual(row["source_product_id"], "brand_category")
            self.assertEqual(row["event_time"].tzinfo, timezone.utc)
            self.assertIsNone(row["session_id"])
            self.assertNotIn("event_id", row)
            self.assertEqual(json.loads(row["source_attributes"])["color"], "blue")
        for raw in adapter.excluded_events:
            self.assertIsNone(adapter.map_event(source_row(raw)))
        for changes in ({"event_id": "new_action"}, {"item_id": None}, {"stime": None}, {"price": float("nan")}):
            with self.assertRaises(ValueError):
                adapter.map_event(source_row(**changes))

    def test_parquet_to_gold_rerun_and_minio_contract(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            raw = root / "raw.parquet"
            silver = root / "silver.parquet"
            rows = [source_row(kind) for kind in MerrecEvents.event_types | dict.fromkeys(MerrecEvents.excluded_events)]
            rows.append(source_row())  # Repeated interaction must keep its own ID.
            pq.write_table(pa.Table.from_pylist(rows), raw)
            with patch("transformer.common.silver.BATCH_SIZE", 2):
                self.assertEqual(write_silver(raw, silver, MerrecEvents()), 4)
            store = LocalLoader(root / "data")
            version = "a" * 64
            key = f"silver/merrec/merrec_events/{version}/events.parquet"
            store.load(key, silver, "application/vnd.apache.parquet")
            dataset = {"name": "merrec", "adapter": "merrec_events"}
            first = build_gold(store, dataset, version)
            second = build_gold(store, dataset, version)
            self.assertEqual(first, second)
            gold = pq.read_table(store.path(first[0]))
            self.assertEqual(gold.schema, GOLD_SCHEMA)
            self.assertEqual(len(set(gold["event_id"].to_pylist())), 4)
            self.assertEqual(gold["position_in_session"].null_count, 4)
            self.assertEqual(validate_gold(store.path(first[0]), "merrec"), first[1])
            self.assertEqual(first[1]["event_counts"], {"product_view": 2, "add_to_cart": 1, "purchase": 1})

            from loader.common.minio_store import MinioLoader
            objects = {key: silver.read_bytes()}

            class MemoryMinio:
                def fget_object(self, bucket, key, path):
                    Path(path).write_bytes(objects[key])

                def fput_object(self, bucket, key, path, content_type):
                    objects[key] = Path(path).read_bytes()

            self.assertEqual(build_gold(MinioLoader(MemoryMinio(), "test"), dataset, version), first)

    def test_missing_parquet_columns(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "bad.parquet"
            pq.write_table(pa.table({"user_id": [1]}), source)
            with self.assertRaisesRegex(ValueError, "Missing source columns"):
                write_silver(source, Path(folder) / "silver.parquet", MerrecEvents())
