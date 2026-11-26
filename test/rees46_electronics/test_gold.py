import tempfile
import gzip
import io
import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from loader.common.minio_store import MinioLoader
from loader.common.local_store import LocalLoader
from pipeline import build_gold, run
from transformer.common.silver import SILVER_SCHEMA
from transformer.common.gold import GOLD_SCHEMA, GOLD_SCHEMA_V3, validate_gold, write_gold
from transformer.datasets.rees46_electronics.gold import Rees46GoldEvents


DATASET = "rees46_electronics"
VERSION = "a" * 64


def silver_row(**changes):
    row = dict.fromkeys(SILVER_SCHEMA.names)
    row.update(event_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
               event_type="product_view", user_id="u1", product_id="p1")
    row.update(changes)
    return row


class GoldTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / "silver.parquet"
        self.destination = Path(self.temp.name) / "gold.parquet"

    def write_source(self, rows, schema=SILVER_SCHEMA):
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), self.source)

    def transform(self):
        return write_gold(self.source, self.destination, Rees46GoldEvents(), DATASET, VERSION)

    def test_schema_duplicates_nulls_and_rerun_across_batch_sizes(self):
        self.write_source([silver_row(), silver_row(), silver_row(event_type="add_to_cart", session_id="s1"),
                           silver_row(event_type="purchase", session_id="s1")])
        with patch("transformer.common.gold.BATCH_SIZE", 2):
            first = self.transform()
        table = pq.read_table(self.destination)
        self.assertEqual(table.schema, GOLD_SCHEMA)
        self.assertEqual(first["rows"], 4)
        self.assertEqual(first["event_counts"], {"product_view": 2, "add_to_cart": 1, "purchase": 1})
        self.assertEqual(len(set(table["event_id"].to_pylist())), 4)
        self.assertEqual(table["session_id"].null_count, 2)
        for name in ("position_in_session", "query_text", "rating_value"):
            self.assertEqual(table[name].null_count, 4)
        self.assertEqual(set(table["event_origin"].to_pylist()), {"observed"})
        self.assertEqual(validate_gold(self.destination, DATASET), first)
        with patch("transformer.common.gold.BATCH_SIZE", 3):
            self.assertEqual(self.transform(), first)
        self.assertTrue(table.equals(pq.read_table(self.destination)))
        write_gold(self.source, self.destination, Rees46GoldEvents(), DATASET, "b" * 64)
        self.assertNotEqual(table["event_id"].to_pylist(), pq.read_table(self.destination)["event_id"].to_pylist())

    def test_preserve_original_ids_and_reject_duplicates(self):
        schema = SILVER_SCHEMA.append(pa.field("event_id", pa.string()))
        self.write_source([silver_row(event_id="native-1"), silver_row(event_id="native-2")], schema)
        self.transform()
        self.assertEqual(pq.read_table(self.destination)["event_id"].to_pylist(), ["native-1", "native-2"])
        self.write_source([silver_row(event_id="same"), silver_row(event_id="same")], schema)
        with self.assertRaisesRegex(ValueError, "Duplicate event_id"):
            self.transform()

    def test_invalid_silver_events_and_empty_input(self):
        for changes in ({"event_type": "view"}, {"product_id": ""}, {"user_id": ""}):
            with self.subTest(changes=changes):
                self.write_source([silver_row(**changes)])
                with self.assertRaises(ValueError):
                    self.transform()
        self.write_source([])
        with self.assertRaisesRegex(ValueError, "No Silver events"):
            self.transform()
        self.write_source([{"event_type": "purchase"}], pa.schema([("event_type", pa.string())]))
        with self.assertRaisesRegex(ValueError, "Missing Silver columns"):
            self.transform()


    def test_readback_rejects_wrong_schema_and_compression(self):
        self.write_source([silver_row()])
        self.transform()
        table = pq.read_table(self.destination)
        pq.write_table(table, self.destination, compression="gzip")
        with self.assertRaisesRegex(ValueError, "Snappy"):
            validate_gold(self.destination, DATASET)
        pq.write_table(table.drop(["query_text"]), self.destination)
        with self.assertRaisesRegex(ValueError, "schema"):
            validate_gold(self.destination, DATASET)


    def test_gold_only_entrypoint_does_not_extract_raw_source(self):
        with patch("sys.argv", ["run_pipeline.py", "--gold-only", "--source-version", VERSION,
                                "--dataset", DATASET]), \
                patch("pipeline.create_loader") as factory, \
                patch("pipeline.build_gold") as build, patch("pipeline.extract") as raw_extract:
            run()
            factory.return_value.ensure_bucket.assert_called_once()
            build.assert_called_once()
            self.assertEqual(build.call_args.args[1]["name"], DATASET)
            self.assertEqual(build.call_args.args[2], VERSION)
            self.assertEqual(build.call_args.kwargs["schema"], GOLD_SCHEMA_V3)
            raw_extract.assert_not_called()

    def test_orchestration_writes_v2_without_changing_v1(self):
        self.write_source([silver_row(), silver_row(event_type="purchase")])
        silver_key = f"silver/{DATASET}/rees46_events/{VERSION}/events.parquet"
        v1_key = f"gold/{DATASET}/{VERSION}/events.parquet"
        v1_content = b"existing-gold-v1"
        objects = {silver_key: self.source.read_bytes(), v1_key: v1_content}
        reads = []

        class MemoryMinio:
            def fget_object(self, bucket, key, path):
                reads.append(key)
                Path(path).write_bytes(objects[key])

            def fput_object(self, bucket, key, path, content_type):
                objects[key] = Path(path).read_bytes()

        loader = MinioLoader(MemoryMinio(), "test")
        dataset = {"name": DATASET, "adapter": "rees46_events"}
        first = build_gold(loader, dataset, VERSION)
        second = build_gold(loader, dataset, VERSION)
        self.assertEqual(first, second)
        self.assertEqual(first[0], f"gold/v2/{DATASET}/{VERSION}/events.parquet")
        self.assertEqual(objects[v1_key], v1_content)
        self.assertEqual(len(objects), 3)
        self.assertEqual(reads, [silver_key, first[0], silver_key, first[0]])

    def test_local_run_downloads_without_credentials_and_reruns(self):
        root = Path(self.temp.name)
        (root / "data").mkdir()
        dataset = {"name": DATASET, "adapter": "rees46_events", "source": "https://example.test/events.csv.gz"}
        (root / "data/datasets.json").write_text(json.dumps({"datasets": [dataset]}), encoding="utf-8")
        content = gzip.compress((
            "event_time,event_type,user_id,product_id,user_session\n"
            "2020-01-01 00:00:00 UTC,view,u1,p1,\n"
            "2020-01-01 00:00:00 UTC,view,u1,p1,\n"
            "2020-01-01 00:00:01 UTC,purchase,u1,p1,s1\n"
        ).encode())
        results = []
        with patch("pipeline.__file__", str(root / "pipeline.py")), \
                patch.dict(os.environ, {}, clear=True), \
                patch("pipeline.load_dotenv", side_effect=AssertionError("Local must not read .env")), \
                patch("pipeline.Minio", side_effect=AssertionError("Local must not connect to MinIO")), \
                patch("extractor.common.source.urlopen", side_effect=lambda *a, **k: io.BytesIO(content)) as download:
            for _ in range(2):
                with patch("sys.argv", ["run_pipeline.py", "--dataset", DATASET, "--storage", "local"]):
                    run()
                gold = list((root / "data/gold").rglob("events.parquet"))
                self.assertEqual(len(gold), 1)
                self.assertIn("gold/v3/", gold[0].as_posix())
                results.append((gold[0], validate_gold(gold[0], DATASET, schema=GOLD_SCHEMA_V3)))
            self.assertEqual(download.call_count, 2)
            with patch("sys.argv", ["run_pipeline.py", "--dataset", DATASET, "--storage", "local",
                                    "--gold-only", "--source-version", gold[0].parent.name]):
                run()
            self.assertEqual(download.call_count, 2)
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0][1]["rows"], 3)
        bronze = list((root / "data/bronze").rglob("*.gz"))
        self.assertEqual(len(bronze), 1)
        self.assertEqual(bronze[0].read_bytes(), content)
        self.assertFalse(list(root.rglob("*.tmp")))

    def test_failed_local_gold_does_not_replace_published_file(self):
        loader = LocalLoader(Path(self.temp.name) / "data")
        dataset = {"name": DATASET, "adapter": "rees46_events"}
        silver_key = f"silver/{DATASET}/rees46_events/{VERSION}/events.parquet"
        self.write_source([silver_row()])
        loader.load(silver_key, self.source, "application/vnd.apache.parquet")
        key, _ = build_gold(loader, dataset, VERSION)
        previous = loader.path(key).read_bytes()
        self.write_source([silver_row(), silver_row(event_type="invalid")])
        loader.load(silver_key, self.source, "application/vnd.apache.parquet")
        with patch("transformer.common.gold.BATCH_SIZE", 1), self.assertRaises(ValueError):
            build_gold(loader, dataset, VERSION)
        self.assertEqual(loader.path(key).read_bytes(), previous)
        with patch("loader.common.local_store.copyfileobj", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                loader.load(key, self.source, "application/vnd.apache.parquet")
        self.assertEqual(loader.path(key).read_bytes(), previous)
        self.assertFalse(list(loader.root.rglob("*.tmp")))
