import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from transformer.common.gold import GOLD_SCHEMA, build_session_key, write_gold
from transformer.common.silver import SILVER_SCHEMA
from transformer.datasets.rees46_electronics.gold import Rees46GoldEvents


class SessionKeyTests(unittest.TestCase):
    def test_tuple_identity_is_deterministic_and_unambiguous(self):
        key = build_session_key("rees46_electronics", "u1", "s1")
        self.assertEqual(key, build_session_key("rees46_electronics", "u1", "s1"))
        self.assertTrue(key.startswith("nutee-session-v1:"))
        self.assertNotEqual(key, build_session_key("rees46_electronics", "u2", "s1"))
        self.assertNotEqual(key, build_session_key("rees46_electronics", "u1", "s2"))
        self.assertNotEqual(key, build_session_key("merrec", "u1", "s1"))

    def test_null_and_empty_ids_produce_null_without_normalizing_valid_ids(self):
        for user_id, session_id in (
            (None, "s1"), ("", "s1"), ("u1", None), ("u1", ""),
        ):
            with self.subTest(user_id=user_id, session_id=session_id):
                self.assertIsNone(build_session_key("dataset", user_id, session_id))
        self.assertNotEqual(
            build_session_key("dataset", "u", "s"),
            build_session_key("dataset", " u", "s"),
        )
        self.assertNotEqual(
            build_session_key("dataset", "u", " "),
            build_session_key("dataset", "u", "  "),
        )

    def test_json_array_encoding_handles_delimiters(self):
        left = build_session_key("a:b", "c", "d")
        right = build_session_key("a", "b:c", "d")
        self.assertNotEqual(left, right)

    def test_invalid_id_types_fail_explicitly(self):
        with self.assertRaisesRegex(TypeError, "user_id"):
            build_session_key("dataset", 1, "s1")
        with self.assertRaisesRegex(TypeError, "session_id"):
            build_session_key("dataset", "u1", 1)

    def test_writer_keys_are_row_order_independent_and_keeps_duplicate_events(self):
        rows = []
        for user_id, session_id in (("u1", "s1"), ("u2", "s1"), ("u1", "s2"), ("u1", "s1")):
            row = dict.fromkeys(SILVER_SCHEMA.names)
            row.update(
                event_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
                event_type="product_view",
                user_id=user_id,
                session_id=session_id,
                product_id="p1",
            )
            rows.append(row)

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            keys_by_order = []
            event_ids_by_order = []
            for name, ordered_rows in (("forward", rows), ("reverse", list(reversed(rows)))):
                source = root / f"{name}-silver.parquet"
                destination = root / f"{name}-gold.parquet"
                pq.write_table(pa.Table.from_pylist(ordered_rows, schema=SILVER_SCHEMA), source)
                stats = write_gold(
                    source, destination, Rees46GoldEvents(), "rees46_electronics", "a" * 64
                )
                gold = pq.read_table(destination)
                self.assertEqual(gold.schema, GOLD_SCHEMA)
                self.assertEqual(stats["rows"], len(rows))
                keys_by_order.append(dict(zip(
                    zip(gold["user_id"].to_pylist(), gold["session_id"].to_pylist()),
                    gold["session_key"].to_pylist(),
                )))
                event_ids_by_order.append(gold["event_id"].to_pylist())

            self.assertEqual(keys_by_order[0], keys_by_order[1])
            self.assertEqual(len(set(event_ids_by_order[0])), len(rows))
            self.assertEqual(len(set(keys_by_order[0].values())), 3)
