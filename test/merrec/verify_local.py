"""Verify a completed MerRec local run, including a Gold-only rerun."""

import argparse
import json
import subprocess
import sys
from hashlib import file_digest
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

from transformer.common.gold import validate_gold
from transformer.datasets.merrec.silver import MerrecEvents


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-version", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    version = args.source_version
    if len(version) != 64 or any(c not in "0123456789abcdef" for c in version):
        parser.error("source-version must be SHA-256")
    bronze = root / "data/bronze/merrec" / version / "000000000030.parquet"
    silver = root / "data/silver/merrec/merrec_events" / version / "events.parquet"
    gold = root / "data/gold/merrec" / version / "events.parquet"
    raw = pq.read_table(bronze, columns=["event_id"])
    source_counts = {x["values"]: x["counts"] for x in pc.value_counts(raw["event_id"]).to_pylist()}
    expected = {normalized: source_counts.get(original, 0) for original, normalized in MerrecEvents.event_types.items()}
    stats = validate_gold(gold, "merrec")
    silver_table = pq.read_table(silver, columns=["event_type", "session_id", "name", "source_attributes"])
    if stats["rows"] != silver_table.num_rows or stats["event_counts"] != expected:
        raise AssertionError("Gold does not match supported Bronze events and Silver")
    with gold.open("rb") as stream:
        first_hash = file_digest(stream, "sha256").hexdigest()
    subprocess.run([sys.executable, "-B", str(root / "run_pipeline.py"), "--dataset", "merrec",
                    "--storage", "local", "--gold-only", "--source-version", version], check=True)
    with gold.open("rb") as stream:
        second_hash = file_digest(stream, "sha256").hexdigest()
    if first_hash != second_hash:
        raise AssertionError("Gold bytes changed on rerun")
    if stats["null_counts"]["session_id"] != silver_table["session_id"].null_count:
        raise AssertionError("Session nulls changed")
    for name in ("position_in_session", "query_text", "rating_value"):
        if stats["null_counts"][name] != stats["rows"]:
            raise AssertionError(f"Unexpected inferred {name}")
    if pc.unique(pq.read_table(gold, columns=["event_origin"])["event_origin"]).to_pylist() != ["observed"]:
        raise AssertionError("Unexpected inferred events")
    report = {"scope": "one configured shard: 20230501/000000000030.parquet",
              "source_version": version, "source_rows": raw.num_rows, "source_counts": source_counts,
              "excluded_counts": {kind: source_counts.get(kind, 0) for kind in sorted(MerrecEvents.excluded_events)},
              "gold_path": str(gold), "gold": stats, "rerun_equal": True,
              "parquet_sha256": second_hash, "minio_validation": "mock only"}
    destination = root / "outputs/merrec/local_acceptance.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
