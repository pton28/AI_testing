"""Rebuild accepted REES46 and MerRec outputs from Bronze in an isolated bucket."""

import argparse
import json
import subprocess
import tempfile
from datetime import datetime, timezone
from hashlib import file_digest, sha256
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
from minio.error import S3Error

from loader.common.minio_store import MinioLoader
from pipeline import create_loader
from test.common.verify_gold_v3_minio import _session_metrics
from transformer.common.gold import GOLD_SCHEMA_V3, validate_gold, write_gold
from transformer.common.model_ready import (
    validate_als_interactions,
    validate_session_sequences,
    write_model_ready,
)
from transformer.common.product_dimension import (
    validate_product_dimension,
    write_product_dimension,
)
from transformer.common.silver import BATCH_SIZE, SILVER_SCHEMA, write_silver
from transformer.registry import ADAPTERS, GOLD_ADAPTERS, PRODUCT_NAME_COLUMNS
from extractor.common.source import read_rows


OFFICIAL_BUCKET = "nutee-recsys"
GOLD_BASELINE_BUCKET = "nutee-sequential-integrity-g4-test-20261126"
PRODUCT_BASELINE_BUCKET = "nutee-product-dimension-g5-test-20261126-1642"
MODEL_READY_BASELINE_BUCKET = "nutee-model-ready-g6-test-20261126-1703b"
RETAINED_G6_ATTEMPT_BUCKET = "nutee-model-ready-g6-test-20261126-1703"
RETAINED_G7_ATTEMPT_BUCKET = "nutee-final-acceptance-g7-test-20261126-1727"
G2_BASELINE_BUCKET = "nutee-session-v2-test-20261126b"
G3_BASELINE_BUCKET = "nutee-event-preservation-g3-test-20261126"
BASELINE_BUCKETS = {
    G2_BASELINE_BUCKET,
    G3_BASELINE_BUCKET,
    GOLD_BASELINE_BUCKET,
    PRODUCT_BASELINE_BUCKET,
    MODEL_READY_BASELINE_BUCKET,
    RETAINED_G6_ATTEMPT_BUCKET,
    RETAINED_G7_ATTEMPT_BUCKET,
}
BASELINES = {
    "rees46_electronics": {
        "version": "cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7",
        "adapter": "rees46_events",
        "bronze_rows": 885129,
        "silver_rows": 885129,
        "gold_rows": 885129,
        "session_keys": 490633,
        "shared_session_keys": 0,
        "ambiguous_events": 7939,
        "products": 53453,
        "als_pairs": 556690,
        "als_interactions": 885129,
        "sequences": 490633,
        "sequence_events": 884964,
        "product_view": 793748,
        "add_to_cart": 54035,
        "purchase": 37346,
    },
    "merrec": {
        "version": "3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca",
        "adapter": "merrec_events",
        "bronze_rows": 605269,
        "silver_rows": 605269,
        "gold_rows": 515601,
        "session_keys": 112859,
        "shared_session_keys": 0,
        "ambiguous_events": 393,
        "products": 431734,
        "als_pairs": 429812,
        "als_interactions": 515601,
        "sequences": 112859,
        "sequence_events": 515601,
        "product_view": 504756,
        "add_to_cart": 10168,
        "purchase": 677,
    },
}
REPORTS = {
    "GĐ2": "reports/session_integrity_g2/minio_integration.json",
    "GĐ3": "reports/event_preservation_g3/integration.json",
    "GĐ4": "reports/sequential_integrity_g4/integration.json",
    "GĐ5": "reports/product_dimension_g5/integration.json",
    "GĐ6": "reports/model_ready_g6/integration.json",
}
REPORT_PATH = Path(__file__).resolve().parents[2] / "reports" / "final_acceptance_g7" / "integration.json"


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def _object_sha256(client, bucket: str, key: str) -> str:
    response = client.get_object(bucket, key)
    digest = sha256()
    try:
        for chunk in iter(lambda: response.read(1024 * 1024), b""):
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()
    return digest.hexdigest()


def _safe_store(
    loader: MinioLoader,
    key: str,
    path: Path,
    content_type: str = "application/vnd.apache.parquet",
) -> str:
    local_sha = _file_sha256(path)
    try:
        loader.client.stat_object(loader.bucket, key)
    except S3Error as error:
        if error.code not in {"NoSuchKey", "NoSuchObject"}:
            raise
        loader.load(key, path, content_type)
    else:
        stored_sha = _object_sha256(loader.client, loader.bucket, key)
        if stored_sha != local_sha:
            raise RuntimeError(f"Object conflict at s3://{loader.bucket}/{key}")
        return stored_sha
    stored_sha = _object_sha256(loader.client, loader.bucket, key)
    if stored_sha != local_sha:
        raise RuntimeError(f"Upload checksum mismatch at s3://{loader.bucket}/{key}")
    return stored_sha


def _snapshot(client, bucket: str) -> dict:
    return {
        item.object_name: {
            "etag": item.etag,
            "size": item.size,
            "last_modified": item.last_modified.isoformat() if item.last_modified else None,
            "version_id": item.version_id,
        }
        for item in client.list_objects(bucket, recursive=True)
    }


def _snapshot_summary(client, bucket: str) -> dict:
    objects = _snapshot(client, bucket)
    digest = json.dumps(objects, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {"object_count": len(objects), "sha256": sha256(digest).hexdigest()}


def _schema_info(schema: pa.Schema, contract: str) -> dict:
    fields = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in schema
    ]
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "contract": contract,
        "fields": fields,
        "fingerprint_sha256": sha256(encoded).hexdigest(),
    }


def _logical_rows_equal(left: Path, right: Path) -> bool:
    with pq.ParquetFile(left) as left_file, pq.ParquetFile(right) as right_file:
        if not left_file.schema_arrow.equals(right_file.schema_arrow, check_metadata=False):
            return False
        if left_file.metadata.num_rows != right_file.metadata.num_rows:
            return False

        left_batches = iter(left_file.iter_batches(batch_size=BATCH_SIZE))
        right_batches = iter(right_file.iter_batches(batch_size=BATCH_SIZE))
        left_batch = next(left_batches, None)
        right_batch = next(right_batches, None)
        left_offset = right_offset = 0

        while left_batch is not None and right_batch is not None:
            row_count = min(
                left_batch.num_rows - left_offset,
                right_batch.num_rows - right_offset,
            )
            if not left_batch.slice(left_offset, row_count).equals(
                right_batch.slice(right_offset, row_count)
            ):
                return False
            left_offset += row_count
            right_offset += row_count
            if left_offset == left_batch.num_rows:
                left_batch = next(left_batches, None)
                left_offset = 0
            if right_offset == right_batch.num_rows:
                right_batch = next(right_batches, None)
                right_offset = 0

        return left_batch is None and right_batch is None


def _count_source_rows(path: Path) -> int:
    with read_rows(path) as (_, rows):
        return sum(1 for _ in rows)


def _find_bronze_key(client, dataset: str, source_version: str) -> str:
    prefix = f"bronze/{dataset}/{source_version}/"
    keys = sorted(
        item.object_name
        for item in client.list_objects(OFFICIAL_BUCKET, prefix=prefix, recursive=True)
    )
    if len(keys) != 1:
        raise RuntimeError(f"Expected one official Bronze object under {prefix}, found {len(keys)}")
    return keys[0]


def _download(client, bucket: str, key: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    client.fget_object(bucket, key, str(destination))
    return destination


def _pipeline_pass(source: Path, dataset: str, source_version: str, directory: Path) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    adapter_name = BASELINES[dataset]["adapter"]
    silver_schema = getattr(ADAPTERS[adapter_name], "silver_schema", SILVER_SCHEMA)
    silver_contract = (
        f"{adapter_name}.silver_schema"
        if hasattr(ADAPTERS[adapter_name], "silver_schema")
        else "SILVER_SCHEMA"
    )
    silver_path = directory / "silver.parquet"
    gold_path = directory / "gold-v3.parquet"
    products_path = directory / "products.parquet"
    als_path = directory / "als-interactions.parquet"
    sequences_path = directory / "session-sequences.parquet"

    silver_rows = write_silver(source, silver_path, ADAPTERS[adapter_name])
    gold_stats = write_gold(
        silver_path,
        gold_path,
        GOLD_ADAPTERS[adapter_name],
        dataset,
        source_version,
        schema=GOLD_SCHEMA_V3,
    )
    product_stats = write_product_dimension(
        silver_path,
        products_path,
        dataset,
        product_name_column=PRODUCT_NAME_COLUMNS[adapter_name],
    )
    model_stats = write_model_ready(
        gold_path,
        products_path,
        als_path,
        sequences_path,
        dataset,
    )
    return {
        "paths": {
            "silver": silver_path,
            "gold_v3": gold_path,
            "product_dimension": products_path,
            "als_interactions": als_path,
            "session_sequences": sequences_path,
        },
        "row_counts": {
            "silver": silver_rows,
            "gold_v3": gold_stats["rows"],
            "product_dimension": product_stats["product_count"],
            "als_interactions": model_stats["als_interactions"]["pair_count"],
            "session_sequences": model_stats["session_sequences"]["sequence_count"],
            "sequence_events": model_stats["session_sequences"]["sequence_event_count"],
        },
        "gold_stats": gold_stats,
        "product_stats": product_stats,
        "model_stats": model_stats,
        "validation": {
            "gold": validate_gold(gold_path, dataset, schema=GOLD_SCHEMA_V3),
            "product_dimension": validate_product_dimension(products_path, dataset),
            "als_interactions": validate_als_interactions(als_path),
            "session_sequences": validate_session_sequences(sequences_path),
        },
        "schema_info": {
            "silver": _schema_info(silver_schema, silver_contract),
            "gold_v3": _schema_info(GOLD_SCHEMA_V3, "GOLD_SCHEMA_V3"),
            "product_dimension": _schema_info(
                pq.read_schema(products_path), "PRODUCT_DIMENSION_SCHEMA"
            ),
            "als_interactions": _schema_info(
                pq.read_schema(als_path), "ALS_INTERACTIONS_SCHEMA"
            ),
            "session_sequences": _schema_info(
                pq.read_schema(sequences_path), "SESSION_SEQUENCES_SCHEMA"
            ),
        },
        "physical_sha256": {
            "silver": _file_sha256(silver_path),
            "gold_v3": _file_sha256(gold_path),
            "product_dimension": _file_sha256(products_path),
            "als_interactions": _file_sha256(als_path),
            "session_sequences": _file_sha256(sequences_path),
        },
    }


def _previous_reports(project_root: Path) -> dict:
    results = {}
    for stage, relative_path in REPORTS.items():
        path = project_root / relative_path
        if not path.is_file():
            raise RuntimeError(f"Missing accepted-stage report {path}")
        report = json.loads(path.read_text(encoding="utf-8"))
        passed = report.get("status") == "PASS"
        if not passed:
            raise RuntimeError(f"{stage} accepted-stage report is not PASS")
        results[stage] = {"status": "PASS", "report": relative_path}
    return results


def _load_baseline_files(client, dataset: str, source_version: str, directory: Path) -> dict:
    adapter = BASELINES[dataset]["adapter"]
    paths = {}
    objects = {
        "silver": (
            GOLD_BASELINE_BUCKET,
            f"silver/{dataset}/{adapter}/{source_version}/events.parquet",
        ),
        "gold_v3": (
            GOLD_BASELINE_BUCKET,
            f"gold/v3/{dataset}/{source_version}/events.parquet",
        ),
        "product_dimension": (
            PRODUCT_BASELINE_BUCKET,
            f"gold/v3/{dataset}/{source_version}/products.parquet",
        ),
        "als_interactions": (
            MODEL_READY_BASELINE_BUCKET,
            f"gold/v3/{dataset}/{source_version}/model_ready/als_interactions.parquet",
        ),
        "session_sequences": (
            MODEL_READY_BASELINE_BUCKET,
            f"gold/v3/{dataset}/{source_version}/model_ready/session_sequences.parquet",
        ),
    }
    for name, (bucket, key) in objects.items():
        paths[name] = _download(client, bucket, key, directory / f"baseline-{name}.parquet")
    return paths


def _make_publication_plan(
    client,
    dataset: str,
    source_version: str,
    bronze_key: str,
    outputs: dict,
    tested_paths: dict,
    temporary: Path,
) -> list:
    adapter = BASELINES[dataset]["adapter"]
    tested_keys = {
        "bronze": bronze_key,
        "silver": f"silver/{dataset}/{adapter}/{source_version}/events.parquet",
        "gold_v3": f"gold/v3/{dataset}/{source_version}/events.parquet",
        "product_dimension": f"gold/v3/{dataset}/{source_version}/products.parquet",
        "als_interactions": f"gold/v3/{dataset}/{source_version}/model_ready/als_interactions.parquet",
        "session_sequences": f"gold/v3/{dataset}/{source_version}/model_ready/session_sequences.parquet",
    }
    plan = []
    for role, tested_key in tested_keys.items():
        proposed_key = tested_key
        preserved_legacy = None
        if role == "silver" and dataset == "merrec":
            legacy_key = f"silver/{dataset}/{adapter}/{source_version}/events.parquet"
            legacy_sha = _object_sha256(
                client,
                OFFICIAL_BUCKET,
                legacy_key,
            )
            legacy_path = _download(
                client, OFFICIAL_BUCKET, legacy_key, temporary / "merrec-legacy-silver.parquet"
            )
            legacy_logical_equal = _logical_rows_equal(
                legacy_path, tested_paths[role]
            )
            preserved_legacy = {
                "key": legacy_key,
                "sha256": legacy_sha,
                "physical_sha256_equal": legacy_sha == outputs[role]["sha256"],
                "logical_content_equal_to_g3_test_output": legacy_logical_equal,
                "action": "PRESERVE; DO NOT OVERWRITE",
            }
            proposed_key = (
                f"silver/{dataset}/{adapter}/{source_version}/"
                "event-preservation-g3/events.parquet"
            )
        try:
            client.stat_object(OFFICIAL_BUCKET, proposed_key)
            existing_sha = _object_sha256(client, OFFICIAL_BUCKET, proposed_key)
            physical_equal = existing_sha == outputs[role]["sha256"]
            if physical_equal:
                logical_equal = True
            elif role == "bronze":
                logical_equal = False
            else:
                existing_path = _download(
                    client,
                    OFFICIAL_BUCKET,
                    proposed_key,
                    temporary / f"official-{role}.parquet",
                )
                logical_equal = _logical_rows_equal(
                    existing_path, tested_paths[role]
                )
            if not logical_equal:
                raise RuntimeError(
                    f"Publication target conflict at s3://{OFFICIAL_BUCKET}/{proposed_key}"
                )
            action = (
                "KEEP_EXISTING_IDENTICAL"
                if physical_equal
                else "KEEP_EXISTING_LOGICALLY_IDENTICAL"
            )
        except S3Error as error:
            if error.code not in {"NoSuchKey", "NoSuchObject"}:
                raise
            existing_sha = None
            physical_equal = None
            logical_equal = None
            action = "PUBLISH_NEW_AFTER_APPROVAL"
        plan.append({
            "role": role,
            "tested_key": tested_key,
            "proposed_official_key": proposed_key,
            "existing_sha256": existing_sha,
            "tested_sha256": outputs[role]["sha256"],
            "physical_sha256_equal": physical_equal,
            "logical_content_equal": logical_equal,
            "action": action,
            "preserved_legacy_object": preserved_legacy,
        })
    return plan


def _process_dataset(client, test_store, dataset: str, baseline: dict, temporary: Path,
                     configuration_sha: str, code_version: dict) -> dict:
    source_version = baseline["version"]
    bronze_key = _find_bronze_key(client, dataset, source_version)
    raw_name = Path(unquote(urlsplit(bronze_key).path)).name
    raw_path = _download(client, OFFICIAL_BUCKET, bronze_key, temporary / "raw" / raw_name)
    source_sha = _file_sha256(raw_path)
    if source_sha != source_version:
        raise RuntimeError(
            f"Bronze checksum differs from source_version for {dataset}: {source_sha}"
        )
    bronze_rows = _count_source_rows(raw_path)
    if bronze_rows != baseline["bronze_rows"]:
        raise AssertionError(f"{dataset} Bronze row count differs from accepted baseline")

    test_bronze_sha = _safe_store(
        test_store, bronze_key, raw_path, "application/octet-stream"
    )
    test_raw_path = _download(
        test_store.client,
        test_store.bucket,
        bronze_key,
        temporary / "test-bronze-input" / raw_name,
    )
    if _file_sha256(test_raw_path) != test_bronze_sha:
        raise RuntimeError(f"Test-bucket Bronze checksum mismatch for {dataset}")
    baselines = _load_baseline_files(client, dataset, source_version, temporary / "baselines")
    first = _pipeline_pass(test_raw_path, dataset, source_version, temporary / "run-1")
    output_keys = {
        "bronze": bronze_key,
        "silver": f"silver/{dataset}/{baseline['adapter']}/{source_version}/events.parquet",
        "gold_v3": f"gold/v3/{dataset}/{source_version}/events.parquet",
        "product_dimension": f"gold/v3/{dataset}/{source_version}/products.parquet",
        "als_interactions": f"gold/v3/{dataset}/{source_version}/model_ready/als_interactions.parquet",
        "session_sequences": f"gold/v3/{dataset}/{source_version}/model_ready/session_sequences.parquet",
    }
    output_paths = {"bronze": raw_path, **first["paths"]}
    object_sha = {"bronze": test_bronze_sha}
    for role, path in output_paths.items():
        if role == "bronze":
            continue
        object_sha[role] = _safe_store(test_store, output_keys[role], path)
    object_rows = {
        "bronze": bronze_rows,
        "silver": first["row_counts"]["silver"],
        "gold_v3": first["row_counts"]["gold_v3"],
        "product_dimension": first["row_counts"]["product_dimension"],
        "als_interactions": first["row_counts"]["als_interactions"],
        "session_sequences": first["row_counts"]["session_sequences"],
    }
    output_objects = {
        role: {
            "key": output_keys[role],
            "sha256": object_sha[role],
            "row_count": object_rows[role],
        }
        for role in output_keys
    }
    output_objects["als_interactions"]["interaction_count_total"] = (
        first["model_stats"]["als_interactions"]["interaction_count"]
    )
    output_objects["session_sequences"]["total_sequence_events"] = (
        first["model_stats"]["session_sequences"]["sequence_event_count"]
    )

    second = _pipeline_pass(test_raw_path, dataset, source_version, temporary / "run-2")
    schema_matches = {
        "silver_baseline": pq.read_schema(first["paths"]["silver"]).equals(
            pq.read_schema(baselines["silver"]), check_metadata=False
        ),
        "silver_rerun": pq.read_schema(first["paths"]["silver"]).equals(
            pq.read_schema(second["paths"]["silver"]), check_metadata=False
        ),
        "gold_v3_baseline": pq.read_schema(first["paths"]["gold_v3"]).equals(
            pq.read_schema(baselines["gold_v3"]), check_metadata=False
        ),
        "gold_v3_rerun": pq.read_schema(first["paths"]["gold_v3"]).equals(
            pq.read_schema(second["paths"]["gold_v3"]), check_metadata=False
        ),
        "product_dimension_baseline": pq.read_schema(first["paths"]["product_dimension"]).equals(
            pq.read_schema(baselines["product_dimension"]), check_metadata=False
        ),
        "product_dimension_rerun": pq.read_schema(first["paths"]["product_dimension"]).equals(
            pq.read_schema(second["paths"]["product_dimension"]), check_metadata=False
        ),
        "als_interactions_baseline": pq.read_schema(first["paths"]["als_interactions"]).equals(
            pq.read_schema(baselines["als_interactions"]), check_metadata=False
        ),
        "als_interactions_rerun": pq.read_schema(first["paths"]["als_interactions"]).equals(
            pq.read_schema(second["paths"]["als_interactions"]), check_metadata=False
        ),
        "session_sequences_baseline": pq.read_schema(first["paths"]["session_sequences"]).equals(
            pq.read_schema(baselines["session_sequences"]), check_metadata=False
        ),
        "session_sequences_rerun": pq.read_schema(first["paths"]["session_sequences"]).equals(
            pq.read_schema(second["paths"]["session_sequences"]), check_metadata=False
        ),
    }
    silver_matches_baseline = _logical_rows_equal(
        baselines["silver"], first["paths"]["silver"]
    )
    silver_matches_rerun = _logical_rows_equal(
        first["paths"]["silver"], second["paths"]["silver"]
    )
    gold_matches_baseline = _logical_rows_equal(
        baselines["gold_v3"], first["paths"]["gold_v3"]
    )
    gold_matches_rerun = _logical_rows_equal(
        first["paths"]["gold_v3"], second["paths"]["gold_v3"]
    )

    previous_reports = {
        name: json.loads(
            (Path(__file__).resolve().parents[2] / path).read_text(encoding="utf-8")
        )
        for name, path in (
            ("G5", REPORTS["GĐ5"]),
            ("G6", REPORTS["GĐ6"]),
        )
    }
    gold_metrics = _session_metrics(first["paths"]["gold_v3"])
    baseline_product = validate_product_dimension(baselines["product_dimension"], dataset)
    baseline_als = validate_als_interactions(baselines["als_interactions"])
    baseline_sequences = validate_session_sequences(baselines["session_sequences"])
    product_logical_equal = (
        first["validation"]["product_dimension"]["content_sha256"]
        == baseline_product["content_sha256"]
        == second["validation"]["product_dimension"]["content_sha256"]
    )
    als_logical_equal = (
        first["validation"]["als_interactions"]["content_sha256"]
        == baseline_als["content_sha256"]
        == second["validation"]["als_interactions"]["content_sha256"]
    )
    sequences_logical_equal = (
        first["validation"]["session_sequences"]["content_sha256"]
        == baseline_sequences["content_sha256"]
        == second["validation"]["session_sequences"]["content_sha256"]
    )

    actual = {
        "bronze_events": bronze_rows,
        "silver_events": first["row_counts"]["silver"],
        "gold_v3_events": first["validation"]["gold"]["rows"],
        "unique_session_keys": gold_metrics["unique_session_keys"],
        "shared_session_keys": gold_metrics["shared_session_keys"],
        "ambiguous_events": gold_metrics["ambiguous_events"],
        "product_dimension_rows": first["row_counts"]["product_dimension"],
        "als_interactions_rows": first["row_counts"]["als_interactions"],
        "als_total_interactions": first["validation"]["als_interactions"]["interaction_count"],
        "session_sequences": first["row_counts"]["session_sequences"],
        "total_sequence_events": first["row_counts"]["sequence_events"],
        "product_coverage": 1.0 if first["model_stats"]["product_coverage_missing"] == 0 else 0.0,
    }
    expected = {
        "bronze_events": baseline["bronze_rows"],
        "silver_events": baseline["silver_rows"],
        "gold_v3_events": baseline["gold_rows"],
        "unique_session_keys": baseline["session_keys"],
        "shared_session_keys": baseline["shared_session_keys"],
        "ambiguous_events": baseline["ambiguous_events"],
        "product_dimension_rows": baseline["products"],
        "als_interactions_rows": baseline["als_pairs"],
        "als_total_interactions": baseline["als_interactions"],
        "session_sequences": baseline["sequences"],
        "total_sequence_events": baseline["sequence_events"],
        "product_coverage": 1.0,
    }
    acceptance = {
        name: {
            "expected": expected[name],
            "actual": actual[name],
            "status": "PASS" if actual[name] == expected[name] else "FAIL",
        }
        for name in expected
    }
    event_counts = first["validation"]["gold"]["event_counts"]
    acceptance["event_distribution_matches"] = {
        "expected": {
            "product_view": baseline["product_view"],
            "add_to_cart": baseline["add_to_cart"],
            "purchase": baseline["purchase"],
        },
        "actual": event_counts,
        "status": "PASS" if event_counts == {
            "product_view": baseline["product_view"],
            "add_to_cart": baseline["add_to_cart"],
            "purchase": baseline["purchase"],
        } else "FAIL",
    }

    logical_comparisons = {
        "silver_multiset_matches_g4_baseline": silver_matches_baseline,
        "silver_multiset_matches_rerun": silver_matches_rerun,
        "gold_v3_multiset_matches_g4_baseline": gold_matches_baseline,
        "gold_v3_multiset_matches_rerun": gold_matches_rerun,
        "product_dimension_matches_g5": product_logical_equal,
        "als_interactions_match_g6": als_logical_equal,
        "session_sequences_match_g6": sequences_logical_equal,
        "schemas_match_baselines": all(schema_matches.values()),
    }
    reproducibility = {
        "logical_content_equal": all(logical_comparisons.values()),
        "physical_sha256_equal": {
            name: first["physical_sha256"][name] == second["physical_sha256"][name]
            for name in first["physical_sha256"]
        },
        "physical_sha256_first_run": first["physical_sha256"],
        "physical_sha256_second_run": second["physical_sha256"],
        "logical_comparisons": logical_comparisons,
    }

    root = Path(__file__).resolve().parents[2]
    config_sha = configuration_sha
    manifest_objects = {
        "bronze": {"key": output_keys["bronze"], "sha256": object_sha["bronze"], "row_count": bronze_rows},
        "silver": {"key": output_keys["silver"], "sha256": object_sha["silver"], "row_count": first["row_counts"]["silver"]},
        "gold_v3": {"key": output_keys["gold_v3"], "sha256": object_sha["gold_v3"], "row_count": first["row_counts"]["gold_v3"]},
        "product_dimension": {"key": output_keys["product_dimension"], "sha256": object_sha["product_dimension"], "row_count": first["row_counts"]["product_dimension"]},
        "als_interactions": {
            "key": output_keys["als_interactions"],
            "sha256": object_sha["als_interactions"],
            "row_count": first["row_counts"]["als_interactions"],
            "interaction_count_total": first["model_stats"]["als_interactions"]["interaction_count"],
        },
        "session_sequences": {
            "key": output_keys["session_sequences"],
            "sha256": object_sha["session_sequences"],
            "row_count": first["row_counts"]["session_sequences"],
            "total_sequence_events": first["model_stats"]["session_sequences"]["sequence_event_count"],
        },
    }
    publication_plan = _make_publication_plan(
        client,
        dataset,
        source_version,
        bronze_key,
        manifest_objects,
        {"bronze": raw_path, **first["paths"]},
        temporary / "publication-plan",
    )
    manifest = {
        "dataset_name": dataset,
        "source_version": source_version,
        "source_sha256": source_sha,
        "schema_versions": {
            "bronze": "source-native",
            "silver": first["schema_info"]["silver"]["contract"],
            "gold": "GOLD_SCHEMA_V3",
            "product_dimension": "PRODUCT_DIMENSION_SCHEMA",
            "als_interactions": "ALS_INTERACTIONS_SCHEMA",
            "session_sequences": "SESSION_SEQUENCES_SCHEMA",
        },
        "schema_fingerprints": first["schema_info"],
        "pipeline_code_version": code_version,
        "configuration_fingerprint": {
            "file": "data/datasets.json",
            "sha256": config_sha,
        },
        "processing_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "test_bucket": test_store.bucket,
        "object_keys": manifest_objects,
        "event_distribution": event_counts,
        "row_counts": {name: details["row_count"] for name, details in manifest_objects.items()},
        "acceptance_status": "PASS" if all(
            criterion["status"] == "PASS" for criterion in acceptance.values()
        ) and reproducibility["logical_content_equal"] else "FAIL",
        "publication_status": "PENDING_USER_APPROVAL",
        "publication_plan": publication_plan,
    }
    manifest_path = temporary / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    manifest_key = f"manifests/{dataset}/{source_version}/manifest.json"
    manifest_sha = _safe_store(
        test_store, manifest_key, manifest_path, "application/json"
    )

    return {
        "source_version": source_version,
        "source_sha256": source_sha,
        "bronze_key": bronze_key,
        "object_keys": {**output_keys, "manifest": manifest_key},
        "object_sha256": {
            **object_sha,
            "manifest": manifest_sha,
        },
        "actual": actual,
        "acceptance_criteria": acceptance,
        "event_distribution": event_counts,
        "gold_session_metrics": gold_metrics,
        "product_dimension_stats": {
            "missing_counts": first["product_stats"]["missing_counts"],
            "metadata_conflicts": first["product_stats"]["metadata_conflicts"],
            "products_with_metadata_conflicts": first["product_stats"]["products_with_metadata_conflicts"],
        },
        "model_ready_stats": first["model_stats"],
        "schema_matches_baselines": schema_matches,
        "logical_comparisons": logical_comparisons,
        "reproducibility": reproducibility,
        "manifest": manifest,
        "publication_plan": publication_plan,
        "accepted_stage_reports": previous_reports,
    }


def run(test_bucket: str) -> dict:
    official = create_loader()
    if official.bucket != OFFICIAL_BUCKET:
        raise RuntimeError(f"Expected configured official bucket {OFFICIAL_BUCKET!r}")
    if not official.client.bucket_exists(OFFICIAL_BUCKET):
        raise RuntimeError("Official MinIO bucket is unavailable")
    for bucket in BASELINE_BUCKETS:
        if not official.client.bucket_exists(bucket):
            raise RuntimeError(f"Accepted or retained baseline bucket {bucket!r} is unavailable")
    if test_bucket == OFFICIAL_BUCKET or test_bucket in BASELINE_BUCKETS:
        raise ValueError("G7 output must use a separate MinIO test bucket")
    if official.client.bucket_exists(test_bucket):
        raise ValueError("G7 requires a new test bucket; refusing to overwrite it")

    project_root = Path(__file__).resolve().parents[2]
    accepted_reports = _previous_reports(project_root)
    config_sha = _file_sha256(project_root / "data" / "datasets.json")
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=project_root, text=True
    ).strip()
    dirty = bool(subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=project_root, text=True
    ).strip())
    code_version = {"git_commit": commit, "dirty_worktree": dirty}

    watched_buckets = {OFFICIAL_BUCKET, *BASELINE_BUCKETS}
    inventories_before = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted(watched_buckets)
    }
    versioning_before = {
        bucket: official.client.get_bucket_versioning(bucket).status
        for bucket in sorted(watched_buckets)
    }

    test_store = MinioLoader(official.client, test_bucket)
    test_store.ensure_bucket()
    results = {}
    with tempfile.TemporaryDirectory(prefix="nutee-g7-final-") as directory:
        root = Path(directory)
        for dataset, baseline in BASELINES.items():
            dataset_dir = root / dataset
            dataset_dir.mkdir()
            results[dataset] = _process_dataset(
                official.client,
                test_store,
                dataset,
                baseline,
                dataset_dir,
                config_sha,
                code_version,
            )
    inventories_after = {
        bucket: _snapshot_summary(official.client, bucket)
        for bucket in sorted(watched_buckets)
    }
    versioning_after = {
        bucket: official.client.get_bucket_versioning(bucket).status
        for bucket in sorted(watched_buckets)
    }
    unchanged = inventories_before == inventories_after
    versioning_unchanged = versioning_before == versioning_after
    all_acceptance = all(
        criterion["status"] == "PASS"
        for result in results.values()
        for criterion in result["acceptance_criteria"].values()
    )
    all_logical = all(
        result["reproducibility"]["logical_content_equal"]
        for result in results.values()
    )
    manifests_pass = all(
        result["manifest"]["acceptance_status"] == "PASS"
        for result in results.values()
    )
    status = (
        "PASS" if all_acceptance and all_logical and manifests_pass
        and unchanged and versioning_unchanged
        and all(result["publication_plan"] for result in results.values())
        and all(item["status"] == "PASS" for item in accepted_reports.values())
        else "FAIL"
    )
    report = {
        "status": status,
        "official_bucket": OFFICIAL_BUCKET,
        "test_bucket": test_bucket,
        "accepted_stage_reports": accepted_reports,
        "pipeline_code_version": code_version,
        "configuration_fingerprint": {
            "file": "data/datasets.json",
            "sha256": config_sha,
        },
        "baseline_inventories_before": inventories_before,
        "baseline_inventories_after": inventories_after,
        "official_and_baseline_objects_unchanged": unchanged,
        "bucket_versioning_before": versioning_before,
        "bucket_versioning_after": versioning_after,
        "bucket_versioning_unchanged": versioning_unchanged,
        "test_objects_retained": True,
        "datasets": results,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if report["status"] != "PASS":
        raise AssertionError("G7 final acceptance failed; see report for details")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-bucket", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.test_bucket), indent=2))


if __name__ == "__main__":
    main()
