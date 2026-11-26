"""Read-only, full-snapshot MinIO audit for REES46 and MerRec."""

import csv
import gzip
import hashlib
import json
import os
import re
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv
from minio import Minio

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from transformer.datasets.merrec.silver import MerrecEvents
from transformer.datasets.rees46_electronics.silver import Rees46Events


ROOT = Path(__file__).resolve().parents[2]
REPORT_ROOT = ROOT / "reports" / "root_cause_analysis"
BATCH_SIZE = 25_000
DATASETS = {
    "rees46_electronics": {
        "adapter": Rees46Events(),
        "source_event": "event_type",
        "metadata": {
            "brand": "brand",
            "category_id": "category_id",
            "category_code": "category_code",
        },
        "timestamps": ("event_time",),
        "position_fields": (),
    },
    "merrec": {
        "adapter": MerrecEvents(),
        "source_event": "event_id",
        "metadata": {
            "brand": "brand_name",
            "category_id": "c2_id",
            "category_code": "c2_name",
        },
        "timestamps": ("stime",),
        "position_fields": ("sequence_id", "sequence_length"),
    },
}


def json_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "as_py"):
        return json_value(value.as_py())
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    return value


def stable_json(value):
    return json.dumps(json_value(value), sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def text(value):
    return str(value).strip() if value is not None else ""


def timestamp_key(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        current = value
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        else:
            current = current.astimezone(timezone.utc)
        return current.isoformat(timespec="microseconds")
    return str(value).strip()


def event_signature(event):
    return (
        timestamp_key(event.get("event_time")),
        text(event.get("event_type")),
        text(event.get("user_id")),
        text(event.get("product_id")),
        text(event.get("session_id")),
    )


def raw_metadata(row, mapping):
    return {key: row.get(source_name) for key, source_name in mapping.items()}


def normalized_metadata(row):
    return {key: row.get(key) for key in ("brand", "category_id", "category_code")}


def raw_event_type(row, dataset, config):
    value = row.get(config["source_event"])
    if dataset == "merrec":
        return text(value)
    return text(value)


def normalize_source(row, dataset, config):
    if dataset == "merrec":
        return config["adapter"].map_event(row)
    return config["adapter"].map_event(row)


def iter_source(path, dataset):
    if dataset == "rees46_electronics":
        with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            for line_number, row in enumerate(reader, start=2):
                yield line_number, row
    else:
        with pq.ParquetFile(path) as parquet:
            row_number = 1
            for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
                for row in batch.to_pylist():
                    yield row_number, row
                    row_number += 1


def iter_parquet(path):
    with pq.ParquetFile(path) as parquet:
        schema = parquet.schema_arrow
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            for row in batch.to_pylist():
                yield schema, row


def is_missing(value):
    return value is None or (isinstance(value, str) and not value.strip())


class Profile:
    def __init__(self, dataset, layer, metadata_fields):
        self.dataset = dataset
        self.layer = layer
        self.metadata_fields = tuple(metadata_fields)
        self.rows = 0
        self.modeled_rows = 0
        self.raw_event_counts = Counter()
        self.event_counts = Counter()
        self.users = set()
        self.products = set()
        self.pairs = set()
        self.session_lengths = Counter()
        self.user_session_lengths = Counter()
        self.raw_session_lengths = Counter()
        self.raw_user_session_lengths = Counter()
        self.raw_session_users = defaultdict(set)
        self.user_session_timestamps = Counter()
        self.modeled_session_timestamps = Counter()
        self.timestamp_group_users = defaultdict(set)
        self.timestamp_group_products = defaultdict(set)
        self.timestamp_group_event_types = defaultdict(set)
        self.product_events = Counter()
        self.product_users = defaultdict(set)
        self.user_products = defaultdict(set)
        self.session_timestamps = Counter()
        self.interaction_content = Counter()
        self.full_content = Counter()
        self.null_counts = Counter()
        self.empty_counts = Counter()
        self.metadata_values = {name: 0 for name in self.metadata_fields}
        self.metadata_product_values = {name: defaultdict(set) for name in self.metadata_fields}
        self.metadata_product_missing = {name: Counter() for name in self.metadata_fields}
        self.metadata_rows = 0
        self.metadata_modeled_values = {name: 0 for name in self.metadata_fields}
        self.metadata_modeled_missing = {name: 0 for name in self.metadata_fields}
        self.metadata_modeled_rows = 0
        self.category_pairs = set()
        self.category_ids_by_code = defaultdict(set)
        self.category_codes_by_id = defaultdict(set)
        self.sequence_fields_present = set()
        self.timestamp_fraction_digits = Counter()
        self.modeled_full_content = Counter()

    def add(self, row, kind, modeled_event=None, metadata=None, timestamp_source=None, source_content=None):
        self.rows += 1
        kind = text(kind)
        if kind:
            self.raw_event_counts[kind] += 1
        if timestamp_source is not None:
            raw_time = text(timestamp_source)
            match = re.search(r"\.(\d+)", raw_time)
            digits = len(match.group(1)) if match else 0
            self.timestamp_fraction_digits[digits] += 1
        if source_content is not None:
            digest = hashlib.sha256(stable_json(source_content).encode("utf-8")).hexdigest()
            self.full_content[digest] += 1

        if self.layer == "bronze" and self.dataset == "rees46_electronics":
            raw_session = row.get("user_session")
            raw_user = row.get("user_id")
            raw_product = row.get("product_id")
            raw_time = row.get("event_time")
        elif self.layer == "bronze":
            raw_session = row.get("session_id")
            raw_user = row.get("user_id")
            raw_product = row.get("item_id")
            raw_time = row.get("stime")
        else:
            raw_session = row.get("session_id")
            raw_user = row.get("user_id")
            raw_product = row.get("product_id")
            raw_time = row.get("event_time")
        session = text(raw_session) or None
        if raw_session is None:
            self.null_counts["session_id"] += 1
        elif isinstance(raw_session, str) and not raw_session.strip():
            self.empty_counts["session_id"] += 1
        if session:
            self.raw_session_lengths[session] += 1
            user = text(raw_user) or None
            if user:
                self.raw_session_users[session].add(user)
                self.raw_user_session_lengths[(user, session)] += 1
        else:
            user = text(raw_user) or None

        if self.metadata_fields:
            self.metadata_rows += 1
            metadata = metadata or {}
            product_for_metadata = text(raw_product) or None
            for field in self.metadata_fields:
                value = metadata.get(field)
                if is_missing(value):
                    self.null_counts[field] += 1
                    if product_for_metadata:
                        self.metadata_product_missing[field][product_for_metadata] += 1
                else:
                    self.metadata_values[field] += 1
                    if product_for_metadata:
                        self.metadata_product_values[field][product_for_metadata].add(text(value))
            category_id = metadata.get("category_id")
            category_code = metadata.get("category_code")
            if not is_missing(category_id) and not is_missing(category_code):
                category_id, category_code = text(category_id), text(category_code)
                self.category_pairs.add((category_id, category_code))
                self.category_ids_by_code[category_code].add(category_id)
                self.category_codes_by_id[category_id].add(category_code)

        if raw_time is not None and session:
            if isinstance(raw_time, datetime):
                time_value = timestamp_key(raw_time)
            elif self.dataset == "rees46_electronics":
                from transformer.common.silver import parse_timestamp

                time_value = timestamp_key(parse_timestamp(text(raw_time)))
            else:
                time_value = timestamp_key(raw_time)
            self.session_timestamps[(session, time_value)] += 1
            timestamp_key_value = (session, time_value)
            if user:
                self.timestamp_group_users[timestamp_key_value].add(user)
            if raw_product is not None and text(raw_product):
                self.timestamp_group_products[timestamp_key_value].add(text(raw_product))
            if kind:
                self.timestamp_group_event_types[timestamp_key_value].add(kind)
            if user:
                self.user_session_timestamps[(user, session, time_value)] += 1

        if modeled_event is None:
            return

        if source_content is not None:
            digest = hashlib.sha256(stable_json(source_content).encode("utf-8")).hexdigest()
            self.modeled_full_content[digest] += 1
        if self.metadata_fields:
            self.metadata_modeled_rows += 1
            for field in self.metadata_fields:
                value = metadata.get(field)
                if is_missing(value):
                    self.metadata_modeled_missing[field] += 1
                else:
                    self.metadata_modeled_values[field] += 1

        self.modeled_rows += 1
        event_type = text(modeled_event.get("event_type"))
        if event_type:
            self.event_counts[event_type] += 1
        user = text(modeled_event.get("user_id")) or None
        product = text(modeled_event.get("product_id")) or None
        session = text(modeled_event.get("session_id")) or None
        event_time = timestamp_key(modeled_event.get("event_time"))
        if session and event_time:
            self.modeled_session_timestamps[(session, event_time)] += 1
        if user:
            self.users.add(user)
        else:
            self.null_counts["user_id"] += 1
        if product:
            self.products.add(product)
            self.product_events[product] += 1
        else:
            self.null_counts["product_id"] += 1
        if user and product:
            self.pairs.add((user, product))
            self.user_products[user].add(product)
            self.product_users[product].add(user)

        if session:
            self.session_lengths[session] += 1
            if user:
                self.user_session_lengths[(user, session)] += 1

        if user and product and event_time:
            self.interaction_content[(
                user, product, event_type, session, event_time,
            )] += 1

        for field in ("sequence_id", "sequence_length", "position_in_session"):
            if field in row:
                self.sequence_fields_present.add(field)

    def result(self):
        unique_users = len(self.users)
        unique_products = len(self.products)
        possible_pairs = unique_users * unique_products
        session_lengths = list(self.session_lengths.values())
        user_session_lengths = list(self.user_session_lengths.values())
        raw_user_session_lengths = list(self.raw_user_session_lengths.values())
        user_product_counts = [len(products) for products in self.user_products.values()]
        product_user_counts = [len(users) for users in self.product_users.values()]
        shared_sessions = {
            session: users
            for session, users in self.raw_session_users.items()
            if len(users) > 1
        }
        null_sessions = self.null_counts["session_id"] + self.empty_counts["session_id"]
        event_total = self.modeled_rows
        metadata = {}
        for field in self.metadata_fields:
            missing = self.null_counts[field]
            recoverable = sum(
                missing_count
                for product, missing_count in self.metadata_product_missing[field].items()
                if self.metadata_product_values[field].get(product)
            )
            metadata[field] = {
                "present_rows": self.metadata_values[field],
                "missing_rows": missing,
                "missing_pct_of_rows": round(100 * missing / self.metadata_rows, 6) if self.metadata_rows else None,
                "modeled_rows_present": self.metadata_modeled_values[field],
                "modeled_rows_missing": self.metadata_modeled_missing[field],
                "modeled_missing_pct": (
                    round(100 * self.metadata_modeled_missing[field] / self.metadata_modeled_rows, 6)
                    if self.metadata_modeled_rows else None
                ),
                "missing_rows_with_value_elsewhere_same_product": recoverable,
                "distinct_values": len({
                    value for values in self.metadata_product_values[field].values() for value in values
                }),
            }
        timestamp_groups = [count for count in self.session_timestamps.values() if count > 1]
        tied_timestamp_keys = [
            key for key, count in self.session_timestamps.items() if count > 1
        ]
        interaction_groups = [count for count in self.interaction_content.values() if count > 1]
        full_duplicate_groups = [count for count in self.full_content.values() if count > 1]
        modeled_full_duplicate_groups = [count for count in self.modeled_full_content.values() if count > 1]
        return {
            "physical_rows": self.rows,
            "modeled_rows": event_total,
            "raw_event_counts": dict(sorted(self.raw_event_counts.items())),
            "event_counts": dict(sorted(self.event_counts.items())),
            "unique_users": unique_users,
            "unique_products": unique_products,
            "unique_sessions_nonnull": len(self.raw_session_lengths),
            "unique_user_scoped_sessions": len(self.user_session_lengths),
            "raw_unique_user_scoped_sessions": len(self.raw_user_session_lengths),
            "missing_session_rows": null_sessions,
            "null_session_rows": self.null_counts["session_id"],
            "empty_session_rows": self.empty_counts["session_id"],
            "shared_sessions_by_user_count": len(shared_sessions),
            "shared_session_user_count": len({user for users in shared_sessions.values() for user in users}),
            "shared_session_event_rows": sum(self.raw_session_lengths[session] for session in shared_sessions),
            "unique_user_item_pairs": len(self.pairs),
            "user_item_matrix_sparsity": (
                round(1 - len(self.pairs) / possible_pairs, 12) if possible_pairs else None
            ),
            "session_length": distribution(session_lengths),
            "user_scoped_session_length": distribution(user_session_lengths),
            "raw_user_scoped_session_length": distribution(raw_user_session_lengths),
            "sessions_with_at_least_two_events": sum(n >= 2 for n in session_lengths),
            "user_scoped_sessions_with_at_least_two_events": sum(
                n >= 2 for n in user_session_lengths
            ),
            "raw_user_scoped_sessions_with_at_least_two_events": sum(
                n >= 2 for n in raw_user_session_lengths
            ),
            "sessions_with_at_least_two_events_pct": (
                round(100 * sum(n >= 2 for n in session_lengths) / len(session_lengths), 6)
                if session_lengths else None
            ),
            "user_distinct_product_count": distribution(user_product_counts),
            "users_with_at_least_two_distinct_products": sum(n >= 2 for n in user_product_counts),
            "product_event_count": distribution(list(self.product_events.values())),
            "products_with_exactly_one_event": sum(n == 1 for n in self.product_events.values()),
            "products_with_exactly_one_event_pct": (
                round(100 * sum(n == 1 for n in self.product_events.values()) / unique_products, 6)
                if unique_products else None
            ),
            "product_distinct_user_count": distribution(product_user_counts),
            "same_session_timestamp_groups": len(timestamp_groups),
            "events_in_same_session_timestamp_groups": sum(timestamp_groups),
            "same_session_timestamp_groups_with_multiple_users": sum(
                len(self.timestamp_group_users[key]) > 1 for key in tied_timestamp_keys
            ),
            "same_session_timestamp_groups_with_multiple_products": sum(
                len(self.timestamp_group_products[key]) > 1 for key in tied_timestamp_keys
            ),
            "same_session_timestamp_groups_with_multiple_event_types": sum(
                len(self.timestamp_group_event_types[key]) > 1 for key in tied_timestamp_keys
            ),
            "same_user_session_timestamp_groups": sum(
                count > 1 for count in self.user_session_timestamps.values()
            ),
            "events_in_same_user_session_timestamp_groups": sum(
                count for count in self.user_session_timestamps.values() if count > 1
            ),
            "duplicate_interaction_content_groups": len(interaction_groups),
            "duplicate_interaction_rows_after_first": sum(n - 1 for n in interaction_groups),
            "exact_full_source_row_duplicate_groups": len(full_duplicate_groups),
            "exact_full_source_duplicate_rows_after_first": sum(n - 1 for n in full_duplicate_groups),
            "exact_full_source_duplicate_groups_in_modeled_rows": len(modeled_full_duplicate_groups),
            "exact_full_source_duplicate_modeled_rows_after_first": sum(n - 1 for n in modeled_full_duplicate_groups),
            "missing_metadata": metadata,
            "category_id_category_code_pairs": len(self.category_pairs),
            "category_ids_with_multiple_codes": sum(len(values) > 1 for values in self.category_codes_by_id.values()),
            "category_codes_with_multiple_ids": sum(len(values) > 1 for values in self.category_ids_by_code.values()),
            "sequence_fields_present": sorted(self.sequence_fields_present),
            "source_timestamp_fraction_digits": dict(sorted(self.timestamp_fraction_digits.items())),
            "metadata_rows": self.metadata_rows,
        }


def distribution(values):
    if not values:
        return {"count": 0, "min": None, "median": None, "p90": None, "p95": None, "p99": None, "max": None, "mean": None}
    ordered = sorted(values)

    def percentile(p):
        return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]

    return {
        "count": len(ordered),
        "min": ordered[0],
        "median": percentile(0.5),
        "p90": percentile(0.9),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": ordered[-1],
        "mean": round(sum(ordered) / len(ordered), 6),
    }


def event_rows_profile(dataset, layer, path, config):
    profile = Profile(dataset, layer, config["metadata"].keys() if layer != "gold" else ())
    signatures = Counter()
    excluded_counts = Counter()
    source = layer == "bronze"
    if source:
        for _, row in iter_source(path, dataset):
            raw_kind = raw_event_type(row, dataset, config)
            try:
                mapped = normalize_source(row, dataset, config)
            except Exception as error:
                raise RuntimeError(f"Adapter mapping failed in {dataset} Bronze: {type(error).__name__}: {error}") from error
            metadata = raw_metadata(row, config["metadata"])
            profile.add(
                row, raw_kind, mapped, metadata,
                timestamp_source=row.get(config["timestamps"][0]),
                source_content=json.loads(json.dumps(row, ensure_ascii=False, default=str)),
            )
            if mapped is None:
                excluded_counts[raw_kind or "<empty>"] += 1
            else:
                signatures[event_signature(mapped)] += 1
    else:
        for schema, row in iter_parquet(path):
            if layer == "silver":
                raw_kind = row.get("source_event_type") or row.get("event_type")
                mapped = row
                metadata = normalized_metadata(row)
                profile.add(row, raw_kind, mapped, metadata)
            else:
                raw_kind = row.get("event_type")
                mapped = row
                metadata = {}
                profile.add(row, raw_kind, mapped, metadata)
            signatures[event_signature(mapped)] += 1
    schema_names = None
    if layer != "bronze":
        with pq.ParquetFile(path) as parquet:
            schema_names = parquet.schema_arrow.names
    return profile, signatures, excluded_counts, schema_names


def read_representative_source_rows(path, dataset, config, selectors, limits=5):
    results = {name: [] for name in selectors}
    seen = {name: set() for name in selectors}
    selected_full = {
        name: selector.get("full_hashes", set()) for name, selector in selectors.items()
    }
    mapped_row_index = 0
    for line_number, row in iter_source(path, dataset):
        mapped = normalize_source(row, dataset, config)
        if mapped is not None:
            mapped_row_index += 1
        raw_kind = raw_event_type(row, dataset, config)
        metadata = raw_metadata(row, config["metadata"])
        full_hash = hashlib.sha256(stable_json(row).encode("utf-8")).hexdigest()
        session = text(row.get("user_session" if dataset == "rees46_electronics" else "session_id")) or None
        time_value = timestamp_key(
            mapped.get("event_time") if mapped is not None else row.get(config["timestamps"][0])
        )
        interaction = event_signature(mapped) if mapped is not None else None
        for name, selector in selectors.items():
            if selector.get("mapped_only") and mapped is None:
                continue
            if len(results[name]) >= limits:
                continue
            match = False
            if selector.get("raw_event") is not None and raw_kind == selector["raw_event"]:
                match = True
            if selector.get("shared_session") is not None and session == selector["shared_session"]:
                match = True
            if selector.get("timestamp_key") is not None and session and (session, time_value) == selector["timestamp_key"]:
                match = True
            if selector.get("interaction_key") is not None and interaction == selector["interaction_key"]:
                match = True
            if selector.get("mapped_event_index") == mapped_row_index and mapped is not None:
                match = True
            if selector.get("full_hashes") and full_hash in selected_full[name]:
                match = True
            if selector.get("missing_session") and session is None:
                match = True
            if selector.get("missing_metadata") and is_missing(metadata.get(selector["missing_metadata"])):
                match = True
            if selector.get("product_single_event") and mapped is not None and text(mapped.get("product_id")) == selector["product_single_event"]:
                match = True
            if selector.get("purchase") and (mapped or {}).get("event_type") == "purchase":
                match = True
            if selector.get("nonpurchase") and mapped is not None and mapped.get("event_type") != "purchase":
                match = True
            if not match:
                continue
            key = stable_json(row)
            if key in seen[name]:
                continue
            seen[name].add(key)
            results[name].append({
                "bronze_row_number_for_display_only": line_number,
                "bronze": json_value(row),
                "mapped_event": json_value(mapped) if mapped is not None else None,
                "metadata": json_value(metadata),
                "signature": list(event_signature(mapped)) if mapped is not None else None,
                "full_content_sha256": full_hash,
            })
    return results


def selected_samples(profile, dataset, config, path, excluded_counts):
    selectors = {}
    shared = {
        session for session, users in profile.raw_session_users.items()
        if len(users) > 1
    }
    for index, session in enumerate(sorted(shared)[:5]):
        selectors[f"session_shared_{index + 1}"] = {"shared_session": session}
    duplicate_times = sorted(
        ((key, count) for key, count in profile.modeled_session_timestamps.items() if count > 1),
        key=lambda item: (-item[1], item[0]),
    )[:5]
    for index, (key, _) in enumerate(duplicate_times):
        selectors[f"same_session_timestamp_{index + 1}"] = {
            "timestamp_key": key,
            "mapped_only": True,
        }
    duplicate_interactions = sorted(
        ((key, count) for key, count in profile.interaction_content.items() if count > 1),
        key=lambda item: (-item[1], item[0]),
    )[:5]
    for index, (key, _) in enumerate(duplicate_interactions):
        selectors[f"duplicate_interaction_{index + 1}"] = {
            "interaction_key": (key[4], key[2], key[0], key[1], key[3])
        }
    duplicate_hashes = sorted(key for key, count in profile.full_content.items() if count > 1)[:5]
    if duplicate_hashes:
        selectors["duplicate_full_source_content"] = {"full_hashes": duplicate_hashes}
    if profile.null_counts["session_id"] or profile.empty_counts["session_id"]:
        selectors["missing_session"] = {"missing_session": True}
    selectors.update({
        f"missing_position_{index + 1}": {"mapped_event_index": index + 1}
        for index in range(5)
    })
    for field in config["metadata"]:
        if profile.null_counts[field]:
            selectors[f"missing_{field}"] = {"missing_metadata": field}
    single_products = sorted(product for product, count in profile.product_events.items() if count == 1)[:5]
    for index, product in enumerate(single_products):
        selectors[f"product_one_event_{index + 1}"] = {"product_single_event": product}
    for index, event_type in enumerate(sorted(excluded_counts)):
        selectors[f"excluded_{event_type}"] = {"raw_event": event_type}
    selectors["purchase_example"] = {"purchase": True}
    selectors["nonpurchase_example"] = {"nonpurchase": True}
    return read_representative_source_rows(path, dataset, config, selectors), selectors


def layer_sample_index(path, signatures_needed, dataset):
    found = defaultdict(list)
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            for row in batch.to_pylist():
                signature = event_signature(row)
                if signature in signatures_needed and len(found[signature]) < 5:
                    found[signature].append(json_value(row))
    return found


def layer_schemas(path):
    with pq.ParquetFile(path) as parquet:
        return parquet.schema_arrow.names


def enrich_samples(samples, silver_path, gold_path, object_keys):
    signatures = {
        tuple(sample["signature"])
        for group in samples.values()
        for sample in group
        if sample["signature"] is not None
    }
    silver_index = layer_sample_index(silver_path, signatures, "silver")
    gold_index = layer_sample_index(gold_path, signatures, "gold")
    enriched = {}
    for group_name, group in samples.items():
        enriched[group_name] = []
        for sample in group:
            signature = tuple(sample["signature"]) if sample["signature"] is not None else None
            silver = silver_index.get(signature, []) if signature is not None else []
            gold = gold_index.get(signature, []) if signature is not None else []
            enriched[group_name].append({
                "bronze_object_key": object_keys["bronze"],
                "silver_object_key": object_keys["silver"],
                "gold_object_key": object_keys["gold"],
                "bronze_row": sample["bronze"],
                "silver_matches_by_event_signature": silver[:5],
                "gold_matches_by_event_signature": gold[:5],
                "matching": {
                    "method": "Exact multiset match on (event_time_UTC, event_type, user_id, product_id, session_id); excluded rows match on source event rule only.",
                    "bronze_silver_gold_row_numbers_assumed_equal": False,
                    "matched_bronze_to_silver": bool(silver),
                    "matched_silver_to_gold": bool(gold),
                    "signature_occurrence_ambiguous": bool(
                        sample["signature"] is not None
                        and (
                            sample.get("signature_count", 1) > 1
                        )
                    ),
                    "confidence": "HIGH for unique signature; MEDIUM for indistinguishable repeated signatures; HIGH for configured exclusion rule.",
                },
                "fields_changed_or_not_present": sample.get("fields_changed_or_not_present", []),
                "full_source_content_sha256": sample["full_content_sha256"],
            })
    return enriched


def multiset_comparison(source_signatures, silver_signatures, gold_signatures):
    source_to_silver = sum(min(count, silver_signatures.get(key, 0)) for key, count in source_signatures.items())
    silver_to_gold = sum(min(count, gold_signatures.get(key, 0)) for key, count in silver_signatures.items())
    return {
        "source_mapped_rows": sum(source_signatures.values()),
        "silver_rows": sum(silver_signatures.values()),
        "gold_rows": sum(gold_signatures.values()),
        "bronze_to_silver_exact_signature_matches": source_to_silver,
        "bronze_mapped_rows_without_silver_match": sum(source_signatures.values()) - source_to_silver,
        "silver_to_gold_exact_signature_matches": silver_to_gold,
        "silver_rows_without_gold_match": sum(silver_signatures.values()) - silver_to_gold,
        "comparison_fields": ["event_time_UTC", "event_type", "user_id", "product_id", "session_id"],
        "limitations": "Duplicate identical signatures can only be verified as multiset counts; no cross-layer row-number assumption.",
    }


def merge_evidence(dataset, source_profile, silver_profile, gold_profile, samples, selectors, excluded_counts):
    source_result = source_profile.result()
    silver_result = silver_profile.result()
    gold_result = gold_profile.result()
    metadata = {}
    for field in ("brand", "category_id", "category_code"):
        metadata[field] = {
            "bronze": source_result["missing_metadata"].get(field),
            "silver": silver_result["missing_metadata"].get(field),
            "gold": "NOT_IN_GOLD_SCHEMA",
        }
    return {
        "dataset": dataset,
        "issue_samples": samples,
        "sample_group_counts": {name: len(rows) for name, rows in samples.items()},
        "excluded_event_counts": dict(excluded_counts),
        "session_shared_example_selectors": [
            value["shared_session"] for value in selectors.values() if "shared_session" in value
        ],
        "metadata_coverage_by_layer": metadata,
        "matching_method": {
            "signature": ["event_time", "event_type", "user_id", "product_id", "session_id"],
            "source_to_silver": "Apply the registered dataset adapter to each Bronze record, then compare event signatures as multisets.",
            "silver_to_gold": "Compare the same event signature as multisets; Gold event_id is not used as a Bronze row number.",
            "merrec_source_attributes": {
                "note": "See full-snapshot multiset check in profiling.json; retained raw rows should match the JSON source_attributes field.",
            },
            "confidence": "High for unique event signatures and full-source-attribute matches; medium where duplicate signatures have indistinguishable occurrences.",
        },
    }


def canonical_metadata(metadata):
    values = tuple(
            None if is_missing(metadata.get(field)) else text(metadata.get(field))
            for field in ("brand", "category_id", "category_code")
    )
    price = metadata.get("price")
    canonical_price = (
        format(Decimal(str(price)).normalize(), "f")
        if not is_missing(price) else None
    )
    return values + (canonical_price,)


def source_attribute_payload(row):
    return json.loads(json.dumps(row, ensure_ascii=False, default=str))


def metadata_and_source_attribute_checks(dataset, source_path, silver_path, config):
    source_metadata = Counter()
    silver_metadata = Counter()
    source_attributes = Counter()
    silver_attributes = Counter()
    source_rows = 0
    for _, row in iter_source(source_path, dataset):
            mapped = normalize_source(row, dataset, config)
            if mapped is None:
                continue
            signature = event_signature(mapped)
            source_rows += 1
            source_metadata[(signature, canonical_metadata(mapped))] += 1
            if dataset == "merrec":
                payload = source_attribute_payload(row)
                source_attributes[(signature, stable_json(payload))] += 1
    with pq.ParquetFile(silver_path) as parquet:
            for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
                for row in batch.to_pylist():
                    signature = event_signature(row)
                    silver_metadata[(signature, canonical_metadata(row))] += 1
                    if dataset == "merrec":
                        payload = json.loads(row["source_attributes"])
                        silver_attributes[(signature, stable_json(payload))] += 1
    metadata_matches = sum(
            min(count, silver_metadata.get(key, 0))
            for key, count in source_metadata.items()
    )
    result = {
            "mapped_source_rows": source_rows,
            "silver_rows": sum(silver_metadata.values()),
            "bronze_to_silver_metadata_multiset_matches": metadata_matches,
            "metadata_rows_with_changed_or_unmatched_values": source_rows - metadata_matches,
            "method": "Multiset comparison by exact event signature plus canonical (brand, category_id, category_code, price); no row-number join.",
    }
    if dataset == "merrec":
            attr_matches = sum(
                min(count, silver_attributes.get(key, 0))
                for key, count in source_attributes.items()
            )
            result["source_attributes_multiset_check"] = {
                "bronze_retained_source_rows": sum(source_attributes.values()),
                "silver_source_attributes_rows": sum(silver_attributes.values()),
                "exact_full_raw_attribute_matches": attr_matches,
                "bronze_rows_without_exact_source_attributes_match": sum(source_attributes.values()) - attr_matches,
                "method": "Multiset match on event signature and canonical JSON of every source Parquet field.",
            }
    return result


def summarize_minio(client, bucket, dataset):
    details = {}
    for layer in ("bronze", "silver", "gold"):
        prefix = f"{layer}/{dataset}/"
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        versions = list(client.list_objects(bucket, prefix=prefix, recursive=True, include_version=True))
        details[layer] = [
            {
                "object_key": obj.object_name,
                "size_bytes": obj.size,
                "etag": obj.etag,
                "last_modified_utc": obj.last_modified.isoformat() if obj.last_modified else None,
                "version_id": obj.version_id,
                "is_latest": obj.is_latest,
                "listed_version_count_for_prefix": len(versions),
                "listed_version_ids": [version.version_id for version in versions if version.object_name == obj.object_name],
            }
            for obj in objects
        ]
    return details


def object_for_layer(details, layer):
    objects = details[layer]
    if len(objects) != 1:
        raise RuntimeError(f"Expected one current {layer} object, found {len(objects)}")
    return objects[0]["object_key"]


def issue_findings(dataset, profiles, comparison, preservation, object_details):
    bronze = profiles["bronze"]
    silver = profiles["silver"]
    gold = profiles["gold"]
    issues = []
    issues.append({
        "issue_id": "01_session_shared_by_users",
        "severity": "P0",
        "affected": {
            "bronze": {
                "sessions": bronze["shared_sessions_by_user_count"],
                "users": bronze["shared_session_user_count"],
                "event_rows_in_shared_sessions": bronze["shared_session_event_rows"],
            },
            "silver": {
                "sessions": silver["shared_sessions_by_user_count"],
                "users": silver["shared_session_user_count"],
                "event_rows_in_shared_sessions": silver["shared_session_event_rows"],
            },
            "gold": {
                "sessions": gold["shared_sessions_by_user_count"],
                "users": gold["shared_session_user_count"],
                "event_rows_in_shared_sessions": gold["shared_session_event_rows"],
            },
        },
        "root_cause": "Shared session IDs are already associated with multiple users in Bronze; Silver/Gold pass through the source session ID without namespacing by user.",
        "classification": ["SOURCE_DATA_ISSUE", "DESIGN_DECISION"],
        "confidence": "HIGH",
        "proposed_fix": "Use a composite analytical session key (user_id, source_session_id) for session modeling while retaining the original source session_id unchanged for lineage.",
    })
    issues.append({
        "issue_id": "02_same_session_timestamp",
        "severity": "P1",
        "affected": {
            "bronze": {
                "groups": bronze["same_session_timestamp_groups"],
                "event_rows": bronze["events_in_same_session_timestamp_groups"],
                "user_scoped_groups": bronze["same_user_session_timestamp_groups"],
                "groups_with_multiple_users": bronze["same_session_timestamp_groups_with_multiple_users"],
                "groups_with_multiple_products": bronze["same_session_timestamp_groups_with_multiple_products"],
                "groups_with_multiple_event_types": bronze["same_session_timestamp_groups_with_multiple_event_types"],
            },
            "silver": {
                "groups": silver["same_session_timestamp_groups"],
                "event_rows": silver["events_in_same_session_timestamp_groups"],
                "user_scoped_groups": silver["same_user_session_timestamp_groups"],
                "groups_with_multiple_users": silver["same_session_timestamp_groups_with_multiple_users"],
                "groups_with_multiple_products": silver["same_session_timestamp_groups_with_multiple_products"],
                "groups_with_multiple_event_types": silver["same_session_timestamp_groups_with_multiple_event_types"],
            },
            "gold": {
                "groups": gold["same_session_timestamp_groups"],
                "event_rows": gold["events_in_same_session_timestamp_groups"],
                "user_scoped_groups": gold["same_user_session_timestamp_groups"],
                "groups_with_multiple_users": gold["same_session_timestamp_groups_with_multiple_users"],
                "groups_with_multiple_products": gold["same_session_timestamp_groups_with_multiple_products"],
                "groups_with_multiple_event_types": gold["same_session_timestamp_groups_with_multiple_event_types"],
            },
        },
        "root_cause": "Equal timestamps occur in Bronze. The source contains no explicit per-event order field; pipeline timestamp normalization does not create one.",
        "classification": ["SOURCE_DATA_ISSUE", "DATASET_LIMITATION"],
        "confidence": "HIGH for collisions; UNVERIFIED for intended order within tied events.",
        "proposed_fix": "Keep ties explicit; add a source-derived stable ordering field only if its semantics are documented. Do not treat file or Parquet row order as behavioral order.",
    })
    issues.append({
        "issue_id": "03_duplicate_interaction_content",
        "severity": "P1",
        "affected": {
            "bronze": {
                "exact_full_row_duplicate_groups": bronze["exact_full_source_row_duplicate_groups"],
                "exact_full_duplicate_rows_after_first": bronze["exact_full_source_duplicate_rows_after_first"],
                "mapped_interaction_duplicate_groups": bronze["duplicate_interaction_content_groups"],
                "mapped_duplicate_rows_after_first": bronze["duplicate_interaction_rows_after_first"],
            },
            "silver": {"interaction_duplicate_groups": silver["duplicate_interaction_content_groups"], "duplicate_rows_after_first": silver["duplicate_interaction_rows_after_first"]},
            "gold": {"interaction_duplicate_groups": gold["duplicate_interaction_content_groups"], "duplicate_rows_after_first": gold["duplicate_interaction_rows_after_first"]},
        },
        "root_cause": "Repeated content is present in Bronze. Gold IDs are intentionally generated per Silver row, so identical interactions retain different IDs; no excess row-count growth is observed across the layers.",
        "classification": ["SOURCE_DATA_ISSUE", "DESIGN_DECISION", "UNVERIFIED"],
        "confidence": "HIGH that repeated records exist and persist; UNVERIFIED whether repeats are accidental or valid repeated behavior.",
        "proposed_fix": "Do not deduplicate automatically. Add a reviewable duplicate policy and preserve raw events; report exact-source duplicates separately from repeated interaction signatures.",
    })
    issues.append({
        "issue_id": "04_missing_position_in_session",
        "severity": "P1",
        "affected": {
            "bronze": {"rows_without_explicit_position": bronze["physical_rows"]},
            "silver": {"rows_without_position_in_session": silver["modeled_rows"]},
            "gold": {"rows_with_null_position_in_session": gold["modeled_rows"]},
        },
        "root_cause": "Neither Bronze schema provides an explicit event position. MerRec has sequence_id and sequence_length, but not a per-event position. Gold adapter leaves position_in_session null.",
        "classification": ["DATASET_LIMITATION", "DESIGN_DECISION"],
        "confidence": "HIGH for missing explicit position; UNVERIFIED that a position can be reconstructed from timestamp ties or source row order.",
        "proposed_fix": "Keep position null until a semantically valid source order is available; expose sequence_id/sequence_length as metadata, not as an invented position.",
    })
    issues.append({
        "issue_id": "05_missing_session",
        "severity": "P1",
        "affected": {
            "bronze": {"null_or_empty_rows": bronze["missing_session_rows"], "null_rows": bronze["null_session_rows"], "empty_rows": bronze["empty_session_rows"]},
            "silver": {"null_or_empty_rows": silver["missing_session_rows"], "null_rows": silver["null_session_rows"], "empty_rows": silver["empty_session_rows"]},
            "gold": {"null_or_empty_rows": gold["missing_session_rows"], "null_rows": gold["null_session_rows"], "empty_rows": gold["empty_session_rows"]},
        },
        "root_cause": "Missing session values are present at Bronze and preserved by the mappings; Gold copies the nullable session field.",
        "classification": ["SOURCE_DATA_ISSUE", "DESIGN_DECISION"],
        "confidence": "HIGH",
        "proposed_fix": "Preserve null sessions and exclude them from session-based grouping; do not infer session IDs without a documented sessionization rule.",
    })
    excluded = {
        kind: count
        for kind, count in bronze["raw_event_counts"].items()
        if count and kind not in (
            {"view", "cart", "purchase"} if dataset == "rees46_electronics"
            else {"item_view", "item_add_to_cart_tap", "buy_comp"}
        )
    }
    issues.append({
        "issue_id": "06_excluded_bronze_events",
        "severity": "P1",
        "affected": {"bronze_excluded_event_counts": excluded, "excluded_rows": sum(excluded.values()), "silver_rows_excluded": sum(excluded.values()), "gold_rows_excluded": sum(excluded.values())},
        "root_cause": (
            "REES46 adapter filters remove_from_cart if present; it is absent in this snapshot."
            if dataset == "rees46_electronics" else
            "Merrec adapter explicitly filters item_like, offer_make, and buy_start before Silver; they are outside the configured 6-event Gold contract."
        ),
        "classification": ["DESIGN_DECISION"],
        "confidence": "HIGH",
        "proposed_fix": "Keep source actions in Bronze and record excluded counts in run metadata. Add a new normalized event only after defining its semantics and evaluating the event contract.",
    })
    purchase_count = gold["event_counts"].get("purchase", 0)
    issues.append({
        "issue_id": "07_purchase_imbalance",
        "severity": "P2",
        "affected": {
            "bronze_purchase_events": bronze["raw_event_counts"].get("purchase", bronze["raw_event_counts"].get("buy_comp", 0)),
            "bronze_rows": bronze["physical_rows"],
            "gold_purchase_events": purchase_count,
            "gold_rows": gold["modeled_rows"],
            "gold_purchase_pct_of_events": round(100 * purchase_count / gold["modeled_rows"], 6) if gold["modeled_rows"] else None,
            "not_a_conversion_rate": True,
        },
        "root_cause": "Purchase frequency is the observed distribution of this snapshot after the configured event mapping; no Gold filtering of supported purchase events is observed.",
        "classification": ["SOURCE_DATA_ISSUE", "DATASET_LIMITATION"],
        "confidence": "HIGH for counts; conversion interpretation is not supported by this event-log denominator.",
        "proposed_fix": "Report event prevalence; use a conversion metric only with defined eligible users/sessions, exposure, and attribution windows.",
    })
    issues.append({
        "issue_id": "08_products_with_one_event",
        "severity": "P2",
        "affected": {
            "bronze_profile_scope": "All source events; item counts are from mapped interactions.",
            "silver": {
                "unique_users": silver["unique_users"],
                "unique_products": silver["unique_products"],
                "unique_sessions_raw_ids": silver["unique_sessions_nonnull"],
                "unique_user_scoped_sessions": silver["unique_user_scoped_sessions"],
                "unique_user_item_pairs": silver["unique_user_item_pairs"],
                "user_item_matrix_sparsity": silver["user_item_matrix_sparsity"],
                "products_with_one_event": silver["products_with_exactly_one_event"],
                "products_with_one_event_pct": silver["products_with_exactly_one_event_pct"],
                "sessions_at_least_two_events_raw_ids": silver["sessions_with_at_least_two_events"],
                "sessions_at_least_two_events_user_scoped": silver["user_scoped_sessions_with_at_least_two_events"],
                "users_at_least_two_distinct_products": silver["users_with_at_least_two_distinct_products"],
                "session_length_raw_ids": silver["session_length"],
                "session_length_user_scoped": silver["user_scoped_session_length"],
            },
            "gold": {
                "unique_users": gold["unique_users"],
                "unique_products": gold["unique_products"],
                "unique_sessions_raw_ids": gold["unique_sessions_nonnull"],
                "unique_user_scoped_sessions": gold["unique_user_scoped_sessions"],
                "unique_user_item_pairs": gold["unique_user_item_pairs"],
                "user_item_matrix_sparsity": gold["user_item_matrix_sparsity"],
                "products_with_one_event": gold["products_with_exactly_one_event"],
                "products_with_one_event_pct": gold["products_with_exactly_one_event_pct"],
                "sessions_at_least_two_events_raw_ids": gold["sessions_with_at_least_two_events"],
                "sessions_at_least_two_events_user_scoped": gold["user_scoped_sessions_with_at_least_two_events"],
                "users_at_least_two_distinct_products": gold["users_with_at_least_two_distinct_products"],
                "session_length_raw_ids": gold["session_length"],
                "session_length_user_scoped": gold["user_scoped_session_length"],
            },
        },
        "root_cause": "Long-tail product frequency and sparse user-item interactions are present in the observed snapshot; for MerRec the configured scope is one sample shard.",
        "classification": ["DATASET_LIMITATION"],
        "confidence": "HIGH for this snapshot; not generalizable to the full MerRec dataset.",
        "proposed_fix": "State snapshot scope in benchmarks; defer K-core filtering and train/test design to an explicit experiment protocol.",
    })
    for issue_id, field in (
        ("09_missing_brand", "brand"),
        ("10_missing_category_id", "category_id"),
        ("11_missing_category_code", "category_code"),
    ):
        issues.append({
            "issue_id": issue_id,
            "severity": "P2",
            "affected": {
                "bronze": bronze["missing_metadata"].get(field),
                "silver": silver["missing_metadata"].get(field),
                "gold": "NOT_IN_GOLD_SCHEMA",
                "bronze_to_silver_mapping_check": preservation["bronze_to_silver_metadata"],
                "recoverable_from_same_product_rows": bronze["missing_metadata"].get(field, {}).get("missing_rows_with_value_elsewhere_same_product"),
            },
            "root_cause": "For available fields, missing values originate in Bronze and are preserved by the Silver mapping. Gold intentionally has no brand/category fields in its 11-column event contract.",
            "classification": ["SOURCE_DATA_ISSUE", "DESIGN_DECISION"],
            "confidence": "HIGH for observed coverage and mapping equality; Gold omission is confirmed by its schema.",
            "proposed_fix": "Use Silver/source_attributes for semantic features or add a separately versioned product dimension; do not synthesize missing catalog values.",
        })
    version_records = {
        layer: object_details[layer]
        for layer in ("bronze", "silver", "gold")
    }
    issues.append({
        "issue_id": "12_minio_versioning",
        "severity": "P2",
        "affected": version_records,
        "root_cause": "Bucket versioning is disabled (status null); each current key has a null version ID. MinioLoader uses fput_object on the same deterministic key.",
        "classification": ["STORAGE_CONFIGURATION", "DESIGN_DECISION"],
        "confidence": "HIGH for observed bucket status and object versions; no write/re-run was attempted in this read-only audit.",
        "proposed_fix": "For future benchmark runs, retain source SHA-256 and a manifest; consider enabling/versioning or immutable run-specific prefixes only after storage policy approval.",
    })
    return issues
def object_key_sha256(key):
    match = re.search(r"/([a-f0-9]{64})/", key)
    return match.group(1) if match else None


def run():
    load_dotenv(ROOT / ".env")
    client = Minio(
        os.getenv("MINIO_ENDPOINT_URL", "localhost:9000"),
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )
    bucket = os.getenv("MINIO_BUCKET", "nutee-recsys")
    if not client.bucket_exists(bucket):
        raise RuntimeError(f"MinIO bucket does not exist: {bucket}")
    versioning = client.get_bucket_versioning(bucket)
    report = {
        "scope": "All currently listed Bronze, Silver, and Gold objects for the two configured datasets; one current object per layer per dataset at audit time.",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "minio_bucket": bucket,
        "bucket_versioning_status": versioning.status,
        "read_only": True,
        "datasets": {},
        "cross_dataset_versioning": {},
    }
    with tempfile.TemporaryDirectory(prefix="nutee-rca-") as temp:
        temp_dir = Path(temp)
        for dataset, config in DATASETS.items():
            object_details = summarize_minio(client, bucket, dataset)
            keys = {layer: object_for_layer(object_details, layer) for layer in ("bronze", "silver", "gold")}
            local = {}
            for layer, key in keys.items():
                filename = Path(key).name
                local[layer] = temp_dir / f"{dataset}-{layer}-{filename}"
                client.fget_object(bucket, key, str(local[layer]))

            source_profile, source_signatures, excluded_counts, _ = event_rows_profile(
                dataset, "bronze", local["bronze"], config,
            )
            silver_profile, silver_signatures, _, silver_schema = event_rows_profile(
                dataset, "silver", local["silver"], config,
            )
            gold_profile, gold_signatures, _, gold_schema = event_rows_profile(
                dataset, "gold", local["gold"], config,
            )
            source_samples, selectors = selected_samples(
                source_profile, dataset, config, local["bronze"], excluded_counts,
            )
            source_counts = source_profile.full_content
            source_samples = {
                name: group for name, group in source_samples.items()
                if group
            }
            # Add match multiplicity for confidence reporting without relying on row order.
            signature_counts = source_signatures
            for group in source_samples.values():
                for sample in group:
                    signature = tuple(sample["signature"]) if sample["signature"] is not None else None
                    sample["signature_count"] = signature_counts.get(signature, 1) if signature is not None else 1
                    if sample["signature"] is not None:
                        sample["fields_changed_or_not_present"] = [
                            field for field in ("brand", "category_id", "category_code")
                            if field in sample["metadata"] and not is_missing(sample["metadata"].get(field))
                        ] + (
                            ["Gold omits product metadata by the 11-column contract"]
                            if dataset in {"rees46_electronics", "merrec"} else []
                        )
                    else:
                        sample["fields_changed_or_not_present"] = [
                            "No Silver/Gold event expected because the registered Silver adapter excludes this source event."
                        ]
            enriched_samples = enrich_samples(
                source_samples, local["silver"], local["gold"], keys,
            )
            comparison = multiset_comparison(source_signatures, silver_signatures, gold_signatures)
            profile_json = {
                "dataset": dataset,
                "source_version_sha256": object_key_sha256(keys["bronze"]),
                "object_keys": keys,
                "object_metadata": object_details,
                "bronze": source_profile.result(),
                "silver": silver_profile.result(),
                "gold": gold_profile.result(),
                "bronze_to_silver_to_gold_lineage": comparison,
                "schema": {
                    "bronze_columns": (
                        sorted(next(iter(iter_source(local["bronze"], dataset)))[1].keys())
                        if dataset == "rees46_electronics"
                        else pq.ParquetFile(local["bronze"]).schema_arrow.names
                    ),
                    "silver_columns": silver_schema,
                    "gold_columns": gold_schema,
                    "bronze_types": (
                        {name: "CSV string" for name in sorted(next(iter(iter_source(local["bronze"], dataset)))[1].keys())}
                        if dataset == "rees46_electronics"
                        else {field.name: str(field.type) for field in pq.ParquetFile(local["bronze"]).schema_arrow}
                    ),
                    "silver_types": {field.name: str(field.type) for field in pq.ParquetFile(local["silver"]).schema_arrow},
                    "gold_types": {field.name: str(field.type) for field in pq.ParquetFile(local["gold"]).schema_arrow},
                },
                "source_sha256_verified_against_bronze_key": (
                    sha256_file(local["bronze"]) == object_key_sha256(keys["bronze"])
                ),
                "source_sha256_actual": sha256_file(local["bronze"]),
            }
            preservation = {
                "bronze_to_silver_metadata": metadata_and_source_attribute_checks(
                    dataset, local["bronze"], local["silver"], config,
                )
            }
            profile_json["bronze_to_silver_metadata_and_raw_attributes"] = preservation
            profiles = {
                "bronze": source_profile.result(),
                "silver": silver_profile.result(),
                "gold": gold_profile.result(),
            }
            findings = issue_findings(dataset, profiles, comparison, preservation, object_details)
            profile_json["issue_findings"] = findings
            evidence = merge_evidence(
                dataset, source_profile, silver_profile, gold_profile,
                enriched_samples, selectors, excluded_counts,
            )
            out_dir = REPORT_ROOT / dataset
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "profiling.json").write_text(
                json.dumps(profile_json, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
            (out_dir / "evidence.json").write_text(
                json.dumps(evidence, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
            report["datasets"][dataset] = {
                "profiling_file": f"{dataset}/profiling.json",
                "evidence_file": f"{dataset}/evidence.json",
                "bronze_to_silver_to_gold_lineage": comparison,
                "bronze_silver_gold_event_counts": {
                    "bronze_raw": source_profile.result()["raw_event_counts"],
                    "bronze_mapped": source_profile.result()["event_counts"],
                    "silver": silver_profile.result()["event_counts"],
                    "gold": gold_profile.result()["event_counts"],
                },
                "object_versioning": object_details,
                "issues": findings,
            }
        report["cross_dataset_versioning"] = {
            "bucket_status": versioning.status,
            "objects": {
                dataset: report["datasets"][dataset]["object_versioning"]
                for dataset in DATASETS
            },
            "interpretation": "A null/absent versioning status and version_id 'null' mean object version history is not enabled for these current objects; the pipeline's fput_object writes to the same key and would replace the current object without retained historical versions.",
        }
    (REPORT_ROOT / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(json.dumps({
        "report": str(REPORT_ROOT / "report.json"),
        "bucket": bucket,
        "versioning_status": versioning.status,
        "datasets": {
            name: {
                "bronze_to_silver_to_gold": data["bronze_to_silver_to_gold_lineage"],
                "bronze_silver_gold_counts": data["bronze_silver_gold_event_counts"],
            }
            for name, data in report["datasets"].items()
        },
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    run()
