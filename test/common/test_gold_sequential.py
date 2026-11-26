import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from transformer.common.gold import (
    GOLD_SCHEMA,
    GOLD_SCHEMA_V3,
    build_session_key,
    order_gold_v3,
    validate_gold,
)


class GoldSequentialTests(unittest.TestCase):
    def test_positions_ties_null_sessions_and_input_order_independence(self):
        time_1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        time_2 = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        key_1 = build_session_key("test", "u1", "s1")
        key_2 = build_session_key("test", "u2", "s2")

        def event(event_id, session_key, session_id, event_time, user_id):
            row = dict.fromkeys(GOLD_SCHEMA.names)
            row.update(
                event_id=event_id,
                source_dataset="test",
                event_type="product_view",
                event_time=event_time,
                user_id=user_id,
                session_id=session_id,
                product_id="p1",
                event_origin="observed",
                session_key=session_key,
            )
            return row

        rows = [
            event("e4", key_1, "s1", time_2, "u1"),
            event("e2", key_1, "s1", time_1, "u1"),
            event("e-null", None, None, time_1, "u3"),
            event("e1", key_1, "s1", time_1, "u1"),
            event("e3", key_2, "s2", time_1, "u2"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_input = root / "input.parquet"
            shuffled_input = root / "shuffled.parquet"
            first_output = root / "gold-v3.parquet"
            shuffled_output = root / "gold-v3-shuffled.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=GOLD_SCHEMA), first_input)
            pq.write_table(pa.Table.from_pylist(list(reversed(rows)), schema=GOLD_SCHEMA), shuffled_input)

            first_stats = order_gold_v3(first_input, first_output, "test")
            shuffled_stats = order_gold_v3(shuffled_input, shuffled_output, "test")
            first = pq.read_table(first_output)
            shuffled = pq.read_table(shuffled_output)

            self.assertEqual(first.schema, GOLD_SCHEMA_V3)
            self.assertEqual(first.num_rows, len(rows))
            self.assertTrue(first.equals(shuffled))
            self.assertEqual(first_stats["content_sha256"], shuffled_stats["content_sha256"])
            self.assertEqual(first_stats, validate_gold(first_output, "test", schema=GOLD_SCHEMA_V3))

            grouped = {}
            for row in first.to_pylist():
                if row["session_key"] is not None:
                    grouped.setdefault(row["session_key"], []).append(row)
                else:
                    self.assertIsNone(row["position_in_session"])
                    self.assertIsNone(row["order_ambiguous"])

            first_session = grouped[key_1]
            self.assertEqual([row["event_id"] for row in first_session], ["e1", "e2", "e4"])
            self.assertEqual([row["position_in_session"] for row in first_session], [1, 2, 3])
            self.assertEqual([row["order_ambiguous"] for row in first_session], [True, True, False])
            self.assertEqual(grouped[key_2][0]["position_in_session"], 1)
            self.assertFalse(grouped[key_2][0]["order_ambiguous"])


if __name__ == "__main__":
    unittest.main()
