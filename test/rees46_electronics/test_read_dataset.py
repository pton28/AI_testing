import gzip
import tempfile
import unittest
import subprocess
import sys
from pathlib import Path

from test.rees46_electronics.read_dataset import PROJECT_DIR, read_purchase_rows, read_sample, read_session_rows


class ReadDatasetTests(unittest.TestCase):
    def test_session_rows_and_cli(self):
        content = (
            "event_time,event_type,user_id,product_id,user_session\n"
            "2020-01-01 00:00:00 UTC,view,1,10,target\n"
            "2020-01-01 00:00:01 UTC,view,2,20,other\n"
            "2020-01-01 00:00:02 UTC,cart,1,10,target\n"
            "2020-01-01 00:00:03 UTC,purchase,1,10,target\n"
            "2020-01-01 00:00:04 UTC,view,3,30,\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            for filename in ("events.csv", "events.csv.gz"):
                path = Path(directory) / filename
                opener = gzip.open if filename.endswith(".gz") else open
                with opener(path, "wt", encoding="utf-8") as stream:
                    stream.write(content)
                rows = read_session_rows(path.name, path.parent, " target ", 10)
                self.assertEqual([row["event_type"] for row in rows], ["view", "cart", "purchase"])
                self.assertEqual(len(read_session_rows(path.name, path.parent, "target", 2)), 2)
                self.assertEqual(read_session_rows(path.name, path.parent, "unknown"), [])
                for session, limit in ((" ", 10), ("target", 0)):
                    with self.assertRaises(ValueError):
                        read_session_rows(path.name, path.parent, session, limit)
            script = PROJECT_DIR / "test" / "rees46_electronics" / "read_dataset.py"
            for options in ([], ["--purchase-only"], ["--session-only", "target"]):
                result = subprocess.run(
                    [sys.executable, "-B", str(script), "--source", str(path), *options],
                    cwd=directory, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                if "--session-only" in options:
                    self.assertIn('"session_rows": 3', result.stdout)

    def test_session_requires_session_column(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.csv"
            path.write_text("event_time,event_type,user_id,product_id\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Missing source columns: user_session"):
                read_session_rows(path.name, path.parent, "target")

    def test_purchase_rows_filter_and_matching_limit(self):
        content = (
            "event_time,event_type,user_id,product_id,price\n"
            "2020-01-01 00:00:00 UTC,view,1,10,1.00\n"
            "2020-01-01 00:00:01 UTC,purchase,1,20,2.50\n"
            "2020-01-01 00:00:02 UTC,cart,1,30,3.00\n"
            "2020-01-01 00:00:03 UTC,purchase,1,40,4.00\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            for filename in ("events.csv", "events.csv.gz"):
                with self.subTest(filename=filename):
                    path = Path(directory) / filename
                    opener = gzip.open if filename.endswith(".gz") else open
                    with opener(path, "wt", encoding="utf-8") as stream:
                        stream.write(content)
                    rows = read_purchase_rows(path.name, path.parent, 1)
                    self.assertEqual([row["product_id"] for row in rows], ["20"])
                    self.assertEqual(rows[0]["price"], "2.50")
                    self.assertEqual(len(read_purchase_rows(path.name, path.parent, 10)), 2)

    def test_purchase_rows_no_matches_and_invalid_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.csv"
            path.write_text("event_time,event_type,user_id,product_id\n2020-01-01 00:00:00 UTC,view,1,2\n", encoding="utf-8")
            self.assertEqual(read_purchase_rows(path.name, path.parent), [])
            with self.assertRaisesRegex(ValueError, "limit must be at least 1"):
                read_purchase_rows(path.name, path.parent, 0)

    def test_gzip_sample_limit_and_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8") as stream:
                stream.write("event_time,event_type,user_id,product_id,price\n")
                stream.write("2020-01-01 00:00:00 UTC,view,1,2,12.34\n")
                stream.write("2020-01-01 00:00:01 UTC,remove_from_cart,1,2,12.34\n")
                stream.write("invalid,unknown,,,\n")
            table, columns, counts = read_sample(path.name, path.parent, 2)
            self.assertIn("event_time", columns)
            self.assertEqual(counts, {"view": 1, "remove_from_cart": 1})
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.to_pylist()[0]["event_type"], "product_view")
            self.assertIsNone(table.to_pylist()[0]["session_id"])
            with self.assertRaisesRegex(ValueError, "CSV line 4"):
                read_sample(path.name, path.parent, 3)

    def test_missing_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.csv"
            path.write_text("user_id\n1\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Missing source columns"):
                read_sample(path.name, path.parent)

    def test_empty_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.csv"
            path.write_text("event_time,event_type,user_id,product_id\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "No supported events"):
                read_sample(path.name, path.parent)


if __name__ == "__main__":
    unittest.main()
