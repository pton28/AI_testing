# RCA Summary — REES46 Electronics & MerRec

## Tóm tắt snapshot

| Dataset | Bronze rows | Silver rows | Gold rows | Unique users | Unique products | Raw session IDs (Gold) | Composite user-sessions (Gold) | User-item pairs | Matrix sparsity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| REES46 Electronics | 885.129 | 885.129 | 885.129 | 407.283 | 53.453 | 490.398 | 490.633 | 556.690 | 99,997443% |
| MerRec | 605.269 | 515.601 | 515.601 | 9.208 | 416.784 | 110.314 | 112.859 | 429.812 | 99,988800% |

All mapped rows matched across Bronze → Silver → Gold by multiset of `(event_time UTC, event_type, user_id, product_id, session_id)`. REES46 mapped 885.129/885.129 rows; MerRec mapped 515.601/605.269 rows. MerRec excludes 89.668 source rows by configured event mapping. Same-signature rows are indistinguishable by this matching method; evidence does not assume cross-layer row-number equality.

Unique raw session IDs may combine multiple users. Profiling therefore also reports session counts and session-length distributions grouped by the analytical composite `(user_id, session_id)`; use those composite metrics for session-based benchmarks.

## Bảng issue

`B / S / G` lần lượt là Bronze / Silver / Gold. Các số là số liệu của snapshot hiện tại; xem profiling/evidence JSON để biết phân phối, object keys và records mẫu.

| Issue ID | Dataset | Bronze | Silver | Gold | Affected rows | Root cause | Evidence | Severity | Proposed fix |
|---|---|---|---|---|---|---|---|---|---|
| 01 session_shared_by_users | REES46 | 214 sessions / 437 users / 1.527 rows | same | same | 1.527 events in shared sessions | Source session IDs reused across users; adapters pass IDs through | [profiling](rees46_electronics/profiling.json), [samples](rees46_electronics/evidence.json) | P0 | Group analytically by `(user_id, source_session_id)`; retain raw ID. |
| 01 session_shared_by_users | MerRec | 2.614 / 2.493 / 28.304 | 2.498 / 2.434 / 23.612 | same as Silver | 28.304 source rows; 23.612 mapped | Collision exists in source; excluded actions account for lower mapped counts | [profiling](merrec/profiling.json), [samples](merrec/evidence.json) | P0 | Same composite key recommendation; preserve source ID. |
| 02 same_session_timestamp | REES46 | 3.327 groups / 8.007 rows; 34 multi-user, 2.557 multi-product, 148 multi-type groups | same | same | 8.007 tied rows | Ties originate in second-precision source timestamps; no event order field (dimension counts overlap) | [profiling](rees46_electronics/profiling.json), [samples](rees46_electronics/evidence.json) | P1 | Keep ties; only add documented source ordering. |
| 02 same_session_timestamp | MerRec | 4.446 / 8.994; 114 multi-user, 139 multi-product, 3.823 multi-type | 270 / 577; 92 / 115 / 85 respectively | same as Silver | 8.994 source rows; 577 mapped | Source ties; Silver/Gold retain only mapped actions (dimensions overlap) | [profiling](merrec/profiling.json), [samples](merrec/evidence.json) | P1 | Keep ties; do not infer order from Parquet row order. |
| 03 duplicate_interaction_content | REES46 | 629 mapped signature groups / 655 extra rows | 629 / 655 | 629 / 655 | 655 extra matching rows | Repeated content originates in source and persists; accidental vs valid repetition is unverified | [profiling](rees46_electronics/profiling.json), [samples](rees46_electronics/evidence.json) | P1 | Preserve events; separate exact-source duplicates from interaction-signature repeats; define review policy. |
| 03 duplicate_interaction_content | MerRec | 507 exact full-row groups / 578 extras; 76 mapped signature groups / 110 extras | 76 / 110 | 76 / 110 | 578 exact-source extras; 110 mapped interaction extras | Repeated content in source; not evidence of ingestion growth | [profiling](merrec/profiling.json), [samples](merrec/evidence.json) | P1 | No automatic deduplication; assess semantics before any policy. |
| 04 missing_position_in_session | REES46 | 885.129 lack explicit position | 885.129 | 885.129 null positions | 885.129 | Source has no explicit position; Gold copies nullable field | [profiling](rees46_electronics/profiling.json), [samples](rees46_electronics/evidence.json) | P1 | Keep null until a semantically valid order is established. |
| 04 missing_position_in_session | MerRec | 605.269 lack per-event position | 515.601 | 515.601 null positions | 515.601 Gold rows | `sequence_id`/`sequence_length` do not encode event position | [profiling](merrec/profiling.json), [samples](merrec/evidence.json) | P1 | Do not derive position from sequence length or file order. |
| 05 missing_session | REES46 | 165 empty values | 165 null | 165 null | 165 | Empty source `user_session` normalized to null, not dropped | [profiling](rees46_electronics/profiling.json), [samples](rees46_electronics/evidence.json) | P1 | Preserve null and exclude from session aggregation; no inferred IDs. |
| 05 missing_session | MerRec | 0 | 0 | 0 | 0 | No missing/empty session in this snapshot | [profiling](merrec/profiling.json) | P1 | No data repair needed; retain validation. |
| 06 excluded_bronze_events | REES46 | `remove_from_cart`: 0 present | 0 excluded | 0 excluded | 0 | Adapter supports view/cart/purchase; remove action absent in this snapshot | [profiling](rees46_electronics/profiling.json), [source](../../transformer/datasets/rees46_electronics/silver.py) | P1 | Keep Bronze and report exclusion counts; define semantics before adding event. |
| 06 excluded_bronze_events | MerRec | item_like 83.571; offer_make 4.260; buy_start 1.837 | excluded | excluded | 89.668 | Explicit Silver adapter filter; actions have distinct meanings | [profiling](merrec/profiling.json), [samples](merrec/evidence.json), [source](../../transformer/datasets/merrec/silver.py) | P1 | Retain source events; add normalized types only with explicit contract/benchmark decision. |
| 07 purchase_imbalance | REES46 | 37.346 / 885.129 | 37.346 | 37.346 / 885.129 (4,2193%) | 37.346 purchase events | Observed event distribution; not a conversion denominator | [profiling](rees46_electronics/profiling.json) | P2 | Report prevalence, not conversion rate. |
| 07 purchase_imbalance | MerRec | 677 / 605.269 | 677 / 515.601 | 677 / 515.601 (0,1313%) | 677 purchase events | Source snapshot distribution after configured exclusions | [profiling](merrec/profiling.json) | P2 | Define eligibility/exposure/attribution before any conversion metric. |
| 08 products_with_one_event | REES46 | 14.738 one-event products | same | same | 14.738 products; sparsity 99,997443% | Long tail and sparse observed user-item matrix | [profiling](rees46_electronics/profiling.json) | P2 | State scope; decide filtering/split in benchmark protocol, not audit. |
| 08 products_with_one_event | MerRec | 359.117 one-event products | same | same | 359.117 products; sparsity 99,988800% | Long tail in this one-shard snapshot | [profiling](merrec/profiling.json) | P2 | Do not generalize to full dataset; defer K-core and split. |
| 09 missing_brand | REES46 | 212.364 (23,9924%) | same | field not in schema | 212.364 | Missing in source; preserved by Silver; no same-product recovery found | [profiling](rees46_electronics/profiling.json), [samples](rees46_electronics/evidence.json) | P2 | Keep missingness; use trusted source catalog if available. |
| 09 missing_brand | MerRec | 118.280 (19,5417%) all rows; 103.821 (20,1359%) mapped | same as mapped Bronze | field not in schema | 103.821 mapped | Source metadata missingness; mapped values match | [profiling](merrec/profiling.json), [samples](merrec/evidence.json) | P2 | Preserve source attributes; no inferred brand values. |
| 10 missing_category_id | REES46 | 0 | 0 | field not in schema | 0 | No missing category IDs in source snapshot | [profiling](rees46_electronics/profiling.json) | P2 | No repair needed; Gold contract intentionally omits metadata. |
| 10 missing_category_id | MerRec | 39.061 (6,4535%) all rows; 34.693 (6,7287%) mapped | same as mapped Bronze | field not in schema | 34.693 mapped | Missing in source; preserved by Silver | [profiling](merrec/profiling.json), [samples](merrec/evidence.json) | P2 | Preserve missing values; only enrich from authoritative catalog. |
| 11 missing_category_code | REES46 | 236.219 (26,6875%) | same | field not in schema | 236.219 | Missing in source; preserved by Silver | [profiling](rees46_electronics/profiling.json), [samples](rees46_electronics/evidence.json) | P2 | Retain missingness; do not infer codes. |
| 11 missing_category_code | MerRec | 39.061 (6,4535%) all rows; 34.693 (6,7287%) mapped | same as mapped Bronze | field not in schema | 34.693 mapped | Missing in source; preserved by Silver | [profiling](merrec/profiling.json), [samples](merrec/evidence.json) | P2 | Retain missingness; do not infer codes. |
| 12 MinIO versioning | REES46 | bucket status null; version ID null | same | same | 3 current objects, no retained versions observed | Version history disabled; deterministic key upload uses `fput_object` | [report](report.json), [loader](../../loader/common/minio_store.py) | P2 | Add run manifest/source hashes; consider versioning or immutable prefixes after approval. |
| 12 MinIO versioning | MerRec | bucket status null; version ID null | same | same | 3 current objects, no retained versions observed | Same storage configuration/write behavior | [report](report.json), [loader](../../loader/common/minio_store.py) | P2 | Same; no MinIO config changed during investigation. |

## Root-cause categories

- **Bronze/source confirmed:** shared session IDs, same-time events, missing session/metadata values, repeated content and observed purchase/distribution characteristics.
- **Silver design choices:** MerRec filters three source actions; REES46 adapter would filter `remove_from_cart`, absent from this snapshot. MerRec ID and metadata mappings are preserved in Silver.
- **Gold design choices:** 11-column event contract omits product metadata; `position_in_session` remains null; generated event IDs identify mapped rows rather than deduplicating behavior.
- **No confirmed Silver/Gold row-loss bug** among mapped events in these snapshots. MerRec exclusions are explicit and fully account for row-count difference.
- **UNVERIFIED:** whether exact repeated records represent accidental source duplicates or legitimate repeated behavior; intended ordering for tied timestamps; whether current MerRec shard represents the whole dataset.

## Model suitability from observed data

- **ALS:** both matrices are extremely sparse; MerRec additionally has 86,16% of products with a single event and only 7.577 users interacting with at least two distinct products. Evaluate filtering and implicit-feedback weighting as experiment choices.
- **Sequential / session-based:** using raw session IDs, REES46 has 140.731 sessions with ≥2 events and MerRec 68.086; using `(user_id, session_id)`, counts are 140.688 and 68.670, respectively. Shared session IDs change session-length statistics. Same-time ties and absent event positions prevent asserting an exact sequence for every session.
- **Semantic recommendation:** Silver retains REES46 brand/category and MerRec product source attributes; Gold omits them. Missing values originate in Bronze. MerRec missing values were not recoverable from another row with the same product ID in this snapshot.
- No K-core filtering, train/test split, or model training was performed.

## Unresolved / UNVERIFIED

1. Repeated content is measurable, but intent (duplicate ingestion versus genuine repeated behavior) is not encoded.
2. No reliable event ordering was found for equal timestamps; physical row order is not treated as behavior.
3. Gold metadata omission is confirmed by its schema, but a separate product dimension is not implemented.
4. Versioning status and current object IDs were read-only observations; no overwrite/re-run test was performed.
5. MerRec profiling covers one current Bronze shard only, not the complete upstream dataset.

## Recommended implementation order (after approval)

1. **P0:** define analytical session identity as `(user_id, source_session_id)` without rewriting raw session IDs.
2. **P1:** specify event inclusion contract for MerRec `item_like`, `offer_make`, `buy_start`; preserve exclusions and source counts in run manifests.
3. **P1:** keep timestamp ties and null position explicit; add source order only with documented semantics.
4. **P1:** publish duplicate statistics separately from any future deduplication policy.
5. **P2:** expose product metadata via a separately versioned dimension; never impute unsupported values.
6. **P2:** version MinIO data or use immutable run prefixes plus source/config/code manifests.

## Code files for a later approved change

- Session key/sequence handling: `transformer/datasets/rees46_electronics/silver.py`, `transformer/datasets/merrec/silver.py`, `transformer/common/gold.py`, and downstream consumers.
- Event semantics/filtering: `transformer/datasets/merrec/silver.py`, corresponding adapter tests and data contract.
- Metadata dimension: dataset Silver adapters, Gold contract/adapter, and related tests.
- Reproducible storage: `loader/common/minio_store.py`, loader configuration, and run manifest handling.
