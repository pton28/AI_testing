"""Read and validate a bounded REES46 sample without uploading to MinIO."""

import argparse
import csv
import gzip
import io
import json
import sys
from collections import Counter
from contextlib import ExitStack, contextmanager
from itertools import islice
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

import pyarrow as pa

PROJECT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_DIR))

from transformer.common.silver import SILVER_SCHEMA
from transformer.datasets.rees46_electronics.silver import Rees46Events

@contextmanager
def open_csv_reader(source: str, base_dir: Path):
    location = urlsplit(source)
    remote = location.scheme.lower() in {"http", "https"}
    with ExitStack() as stack:
        raw = stack.enter_context(
            urlopen(source, timeout=60) if remote else (
                base_dir / source).open("rb")
        )
        compressed = (
            location.path if remote else source).lower().endswith(".gz")
        stream = stack.enter_context(gzip.GzipFile(
            fileobj=raw)) if compressed else raw
        text = stack.enter_context(io.TextIOWrapper(
            stream, encoding="utf-8-sig", newline=""))
        reader = csv.DictReader(text)
        columns = reader.fieldnames or []
        missing = Rees46Events.required_columns.difference(columns)
        if missing:
            raise ValueError(
                f"Missing source columns: {', '.join(sorted(missing))}")
        yield reader


def read_purchase_rows(source: str, base_dir: Path, limit: int = 1000) -> list[dict]:
    """Return up to limit purchase rows, preserving original CSV fields.

    The limit counts matching rows, not scanned source rows. Scan until enough
    purchases are found or EOF; return an empty list when none match.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    with open_csv_reader(source, base_dir) as reader:
        purchases = (row for row in reader if (
            row.get("event_type") or "").strip() == "purchase")
        return list(islice(purchases, limit))


def read_session_rows(source: str, base_dir: Path, session_id: str, limit: int = 1000) -> list[dict]:
    """Return up to limit rows matching user_session, in source order.

    The limit counts matching rows, not scanned source rows. Scan until enough
    session rows are found or EOF; return an empty list when none match.
    All event types are included, even when matching rows are nonconsecutive.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    session_id = session_id.strip()
    if not session_id:
        raise ValueError("session_id must not be empty")
    with open_csv_reader(source, base_dir) as reader:
        if "user_session" not in (reader.fieldnames or []):
            raise ValueError("Missing source columns: user_session")
        session_rows = (row for row in reader if (
            row.get("user_session") or "").strip() == session_id)
        return list(islice(session_rows, limit))


def read_sample(source: str, base_dir: Path, limit: int = 1000):
    if limit < 1:
        raise ValueError("limit must be at least 1")
    adapter = Rees46Events()
    with open_csv_reader(source, base_dir) as reader:
        columns = reader.fieldnames or []
        events = []
        counts = Counter()
        for row in islice(reader, limit):
            try:
                event = adapter.map_event(row)
            except ValueError as error:
                raise ValueError(
                    f"Invalid source event at CSV line {reader.line_num}: {error}") from error
            counts[(row.get("event_type") or "").strip()] += 1
            if event is not None:
                events.append(event)
    if not events:
        raise ValueError("No supported events found in the sample")
    table = pa.Table.from_pylist(events, schema=SILVER_SCHEMA)
    table.validate(full=True)
    return table, columns, dict(counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=1000,
                        help="Maximum rows: source rows in sample mode, matching rows in filter modes (default: 1000)")
    parser.add_argument(
        "--source", help="Override URL or local path relative to data/")
    filters = parser.add_mutually_exclusive_group()
    filters.add_argument("--purchase-only", action="store_true",
                        help="Read raw purchase rows; --limit counts matching rows")
    filters.add_argument("--session-only", "--session-id", dest="session_only", metavar="SESSION_ID",
                         help="Read raw rows matching user_session; --limit counts matching rows")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.session_only is not None and not args.session_only.strip():
        parser.error("session ID must not be empty")
    config_path = PROJECT_DIR / "data" / "datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    dataset = next(
        item for item in config["datasets"] if item["name"] == "rees46_electronics")
    if dataset["adapter"] != "rees46_events":
        raise ValueError("Expected adapter rees46_events")
    source = args.source or dataset["source"]
    print(f"Dataset: {dataset['name']}\nSource: {source}", flush=True)
    if args.purchase_only:
        rows = read_purchase_rows(source, config_path.parent, args.limit)
        print(json.dumps({"purchase_rows": len(rows),
              "rows": rows}, indent=2, ensure_ascii=False))
        return
    elif args.session_only:
        rows = read_session_rows(source, config_path.parent, args.session_only, args.limit)
        print(json.dumps({"session_id": args.session_only.strip(), "session_rows": len(rows),
              "rows": rows}, indent=2, ensure_ascii=False))
        return
    table, columns, counts = read_sample(
        source, config_path.parent, args.limit)
    print(json.dumps({
        "columns": columns,
        "source_rows": sum(counts.values()),
        "event_counts": counts,
        "normalized_rows": table.num_rows,
        "skipped_rows": sum(counts.values()) - table.num_rows,
        "preview": table.slice(0, 5).to_pylist(),
    }, indent=2, ensure_ascii=False, default=str))
    print("PASS: sampled rows match the pipeline schema (not a full dataset validation)")


if __name__ == "__main__":
    main()
