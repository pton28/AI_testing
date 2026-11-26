# Root Cause Analysis — MerRec

## Snapshot và phương pháp

- Bronze: `bronze/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/000000000030.parquet`
- Silver: `silver/merrec/merrec_events/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/events.parquet`
- Gold: `gold/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/events.parquet`
- Bronze SHA-256 khớp source version trong key. Snapshot hiện tại chỉ gồm một shard Parquet; kết quả không đại diện mặc định cho toàn MerRec.
- Lineage sử dụng multiset exact signature `(event_time UTC, event_type, user_id, product_id, session_id)`. 515.601 mapped Bronze rows, Silver rows và Gold rows match đầy đủ; 89.668 Bronze rows bị adapter loại có chủ đích. Không dùng row number làm lineage.
- MerRec Bronze `stime` là Parquet `timestamp[us]`; các giá trị của snapshot không có fractional digits. Pipeline gắn UTC theo semantics nguồn trong adapter. Evidence samples nằm trong [evidence.json](evidence.json); profile đầy đủ ở [profiling.json](profiling.json).

## Khả năng huấn luyện trên snapshot

| Metric | Gold snapshot |
|---|---:|
| Events | 515.601 |
| Users / products | 9.208 / 416.784 |
| Raw session IDs / composite `(user_id, session_id)` sessions | 110.314 / 112.859 |
| User-item pairs | 429.812 |
| User-item matrix sparsity | 99,988800% |
| Products có đúng một event | 359.117 (86,1638%) |
| Sessions ≥2 events (raw IDs / composite) | 68.086 / 68.670 |
| Users ≥2 distinct products | 7.577 |
| Session length median / p90 / max (raw IDs) | 2 / 11 / 238 |
| Session length median / p90 / max (composite) | 2 / 11 / 238 |
| Event types | product_view 504.756; add_to_cart 10.168; purchase 677 |
| Purchase share | 677/515.601 = 0,1313%; không phải conversion rate |

## Findings

| Issue | Snapshot evidence | Root cause, classification, confidence | Impact / proposed fix |
|---|---|---|---|
| **01 — session_shared_by_users (P0)** | Bronze: 2.614 IDs, 2.493 users, 28.304 rows. Silver/Gold: 2.498 IDs, 2.434 users, 23.612 rows. Mẫu trong [evidence.json](evidence.json), group `session_shared_*`. | Shared source session IDs persisted for mapped events; excluded events explain reduced Silver count. [MerrecEvents.map_event, dòng 25](../../../transformer/datasets/merrec/silver.py#L25) copies source session (dòng 52). **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Group analytics by `(user_id, source_session_id)` while retaining source ID. |
| **02 — same_session_timestamp (P1)** | Bronze: 4.446 groups / 8.994 rows; 4.334 same-user groups. 114 groups have multiple users, 139 multiple products, and 3.823 multiple event types. Silver/Gold: 270 / 577, 178 same-user groups; 92 groups have multiple users, 115 multiple products, 85 multiple event types. Dimensions overlap. Bronze type `timestamp[us]`, but all snapshot values have 0 fractional digits. Mẫu trong [evidence.json](evidence.json), group `same_session_timestamp_*`. | Ties originate in source timestamps; exclusions reduce observed Silver/Gold ties. [MerrecEvents.map_event, dòng 25](../../../transformer/datasets/merrec/silver.py#L25) preserves timestamp precision and assigns UTC (dòng 48); no per-event position. **SOURCE_DATA_ISSUE, DATASET_LIMITATION — HIGH for ties; UNVERIFIED for intended event order.** | Preserve timestamp ties; do not infer order from file/Parquet row order. |
| **03 — duplicate_interaction_content (P1)** | Bronze có 507 exact full-row groups / 578 extra rows; trong mapped events có 76 signature groups / 110 extra rows, giữ nguyên ở Silver/Gold. [Profiling](profiling.json), mapped samples [evidence](evidence.json), groups `duplicate_interaction_*`. | Repetitions exist in Bronze and persist for supported events; [stable_event_id, dòng 51](../../../transformer/common/gold.py#L51) generates row IDs, not deduplication. Source `event_id` is an action name, not a unique identifier ([MerrecEvents.map_event, dòng 25](../../../transformer/datasets/merrec/silver.py#L25)). **SOURCE_DATA_ISSUE / DESIGN_DECISION; accidental vs valid repetition UNVERIFIED.** | No automatic deduplication; retain raw attributes and classify duplicate signatures separately. |
| **04 — missing_position_in_session (P1)** | No per-event position in 605.269 Bronze rows; Silver has `sequence_id`/`sequence_length`, but no position; all 515.601 Gold `position_in_session` are null. Mẫu Bronze/Silver/Gold trong [evidence.json](evidence.json), groups `missing_position_*`. | `sequence_id` / `sequence_length` are the only sequence fields added by [MerrecEvents, dòng 16](../../../transformer/datasets/merrec/silver.py#L16), not event ordinals. [NormalizedGoldEvents.map_event, dòng 45](../../../transformer/common/gold.py#L45) copies optional position only. **DATASET_LIMITATION, DESIGN_DECISION — HIGH.** | Do not synthesize position from sequence length, timestamp ties or row order. |
| **05 — missing_session (P1)** | 0 null/empty session values in Bronze, Silver and Gold. | Not present in this snapshot. **HIGH.** | No repair needed; retain validation. |
| **06 — excluded_bronze_events (P1)** | Bronze: `item_like` 83.571; `offer_make` 4.260; `buy_start` 1.837. Total 89.668/605.269 excluded. Mapped counts: item_view 504.756, item_add_to_cart_tap 10.168, buy_comp 677. Samples in [evidence.json](evidence.json), groups `excluded_*`. | [MerrecEvents.event_types / excluded_events, dòng 14–15](../../../transformer/datasets/merrec/silver.py#L14) maps the three supported actions; `map_event`, dòng 25, returns `None` for excluded actions. **DESIGN_DECISION — HIGH.** | These are semantically different actions; do not merge into purchase/view without domain approval. Keep Bronze and exclusion counts in manifests; assess value for research before extending contract. |
| **07 — purchase_imbalance (P2)** | 677 purchase events in Bronze; 677/515.601 Gold events (0,1313%). | Source event distribution among mapped actions; no evidence supported purchases were dropped in Silver/Gold. **SOURCE_DATA_ISSUE, DATASET_LIMITATION — HIGH.** | Not a conversion rate. Define exposed/eligible denominator and attribution window first. |
| **08 — products_with_one_event (P2)** | 359.117 products (86,1638%) have one event; 429.812 pairs, sparsity 99,988800%; 68.086 sessions ≥2 events; 7.577 users ≥2 products. | Long-tail pattern in one current sample shard. **DATASET_LIMITATION — HIGH only for this snapshot.** | Do not generalize to complete MerRec; defer K-core and split to a declared benchmark design. |
| **09 — missing_brand (P2)** | Bronze: 118.280 (19,5417%) missing across all rows; mapped subset: 103.821/515.601 (20,1359%). Silver matches mapped source metadata; 0 same-product recovery. Gold has no field. Samples in [evidence.json](evidence.json), group `missing_brand`. | Missing in source, not a transform loss. All 515.601 retained rows preserve exact raw attributes through `source_attributes` ([MerrecEvents.map_event, dòng 63](../../../transformer/datasets/merrec/silver.py#L63)); Gold intentionally omits product metadata ([GOLD_SCHEMA, dòng 16](../../../transformer/common/gold.py#L16)). **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Use retained Silver source attributes or trusted catalog; do not impute. |
| **10 — missing_category_id (P2)** | Bronze: 39.061/605.269 (6,4535%); mapped Bronze/Silver: 34.693/515.601 (6,7287%). 0 same-product recovery. Samples in [evidence.json](evidence.json), group `missing_category_id`. | Missing in source and preserved in Silver ([mapping, dòng 53](../../../transformer/datasets/merrec/silver.py#L53)). Raw attributes retained for mapped events; Gold has no field. **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Enrich only from authoritative product catalog; preserve null otherwise. |
| **11 — missing_category_code (P2)** | Same missing row counts as category ID: 39.061 all Bronze; 34.693 mapped. 0 same-product recovery. No category ID maps to multiple codes; 194 codes map to multiple IDs. Samples in [evidence.json](evidence.json), group `missing_category_code`. | Missing in source and preserved by mapping `c2_name → category_code` ([mapping, dòng 54](../../../transformer/datasets/merrec/silver.py#L54)); code/name is not a unique identifier for category ID. **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Do not reconstruct ID from code. Keep ID/code separately and use trusted taxonomy if enriching. |
| **12 — MinIO versioning (P2)** | Bucket status `null`; current three object keys have `version_id=null`; no historical versions observed under prefixes at audit time. | [MinioLoader.load, dòng 22](../../../loader/common/minio_store.py#L22) uses `fput_object` against deterministic key. **STORAGE_CONFIGURATION, DESIGN_DECISION — HIGH for observed state; no overwrite test performed.** | Consider object versioning or immutable run prefixes and retain source/config/code manifest after approval. |

## Metadata và raw attribute preservation

- Brand: 486.989/605.269 present in Bronze; 411.780/515.601 present in the mapped slice. Silver exactly matches these mapped values.
- Category ID: 566.208/605.269 present in Bronze; 480.908/515.601 mapped rows present.
- Category code: same coverage as category ID; these are mapped from `c2_id` / `c2_name`.
- No missing brand/category values had a same-product value elsewhere in this one-shard snapshot.
- All 515.601 mapped Bronze rows matched Silver `source_attributes` by exact multiset comparison over all source Parquet columns and the event signature. The normalized `brand`, `category_id`, `category_code` and `price` values were also checked between Bronze mapping and Silver. This verifies preservation for included events, not excluded rows.
- Gold contains no metadata or price fields by its 11-column event contract. The 194 category codes associated with multiple IDs indicate names/codes are not guaranteed unique IDs; this alone does not establish a taxonomy defect.

## 89.668 excluded rows and Recommendation impact

`item_like`, `offer_make`, and `buy_start` may represent distinct user intent (preference, offer, and purchase funnel initiation). Excluding them means Gold has no signal for those actions; treating them as purchase or add-to-cart could distort implicit feedback and purchase prevalence. No semantic equivalence is assumed. The correct decision depends on benchmark objective and an explicitly versioned event contract.

## Ảnh hưởng mô hình và giới hạn

- **ALS:** sparse matrix plus 86,16% one-event products and only 7.577 users with ≥2 distinct products; severe long-tail for the observed shard. No K-core or training run was performed.
- **Sequential/session-based:** 68.086 sessions have ≥2 events. Shared IDs should be user-scoped. No event position is available, and 577 mapped rows fall in same-session timestamp groups; exact sequence order is not verified for ties.
- **Semantic:** mapped rows retain every source Parquet field in Silver `source_attributes`; Gold discards product metadata. Coverage gaps are source-side.
- **Purchase:** 677/515.601 is event-type share only, not conversion.
- No confirmed transformation defect among mapped rows; the 89.668 row difference is completely explained by the configured event exclusions.
- Same-signature duplicates cannot be joined to a unique physical row across layers; exact duplicate intent and timestamp tie order remain **UNVERIFIED**.
