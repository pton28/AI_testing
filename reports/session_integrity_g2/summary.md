# GĐ2 – Session Integrity Report

## Status

**TESTED; awaiting user acceptance.** Gold V2 is not published to the official
bucket. Gold V1 and the GĐ1 RCA reports are retained.

## Implementation

`GOLD_SCHEMA_V1` preserves the previous 11-column schema. Gold V2 appends the
nullable string field `session_key` as column 12. The shared Gold writer builds
the key from the exact tuple `(source_dataset, user_id, session_id)`, encoded as
a compact JSON array with UTF-8 and SHA-256:

```text
nutee-session-v1:<64-character-sha256>
```

Null or empty user/session IDs produce a null key. The helper does not trim,
case-fold, or otherwise normalize IDs. `event_id` generation and all existing
Gold fields are unchanged. `validate_gold` validates V2 by default; callers must
select `GOLD_SCHEMA_V1` explicitly to validate historical V1 files.

Pipeline output is now `gold/v2/<dataset>/<source_version>/events.parquet`;
the prior V1 key remains separate.

## Files changed

- `transformer/common/gold.py` — V1/V2 schemas, shared key generation and validation.
- `pipeline.py` — V2 object key.
- `test/common/test_gold_contract.py`, `test/common/test_session_key.py` —
  contract, edge-case and determinism tests.
- `test/rees46_electronics/test_gold.py`, `test/merrec/test_pipeline.py` —
  pipeline and duplicate-event regression coverage.
- `test/common/verify_gold_v2_minio.py` — isolated-bucket acceptance runner.
- `test/rees46_electronics/verify_gold_minio.py` — safe compatibility entrypoint.
- `test/merrec/verify_local.py` — V2 path and report labels.
- `README.md` — object paths and schema versions.

## Test results

- Full unit/regression suite: **PASS** (`python -m unittest discover -s test`;
  30 tests).
- Targeted contract/session tests: **PASS** (9 tests after adding the
  cross-field validation case).
- `py_compile` and `git diff --check`: **PASS**.
- MinIO integration: **PASS** for both datasets. Full-snapshot evidence is in
  [minio_integration.json](./minio_integration.json).

## Acceptance results

| Dataset | Gold events | Unique non-null keys | Null keys | Shared keys | Lost/added events | Changed event/session IDs | Hash collisions | 11-field multiset |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| REES46 | 885,129 / 885,129 | 490,633 / 490,633 | 165 / 165 | 0 / 0 | 0 / 0 | 0 / 0 | 0 | PASS |
| MerRec | 515,601 / 515,601 | 112,859 / 112,859 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 | PASS |

Values show actual / expected where numeric. The runner also confirmed row
counts and event-type distributions match Silver and Gold V1, and checked
490,633 REES46 and 112,859 MerRec valid tuple-to-key mappings.

| Dataset | Gold V2 test object | Gold content SHA-256 |
|---|---|---|
| REES46 | `gold/v2/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/events.parquet` | `c88f5fd9a7d75bd7f971b6e4226a776d9cd97b58c3f7215570dbc18e4ec8f336` |
| MerRec | `gold/v2/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/events.parquet` | `15c69bd6987ef9c49bf0cb9d44354fb310ee5a890dd05717209805c48520bb20` |

## MinIO test safety and evidence

The successful run used test bucket `nutee-session-v2-test-20261126b`, retaining
both Silver inputs and Gold V2 outputs. The official bucket `nutee-recsys`
contained 6 objects before and after; its inventory SHA-256 was identical:
`5f074564e6a8ae3e98bd2ce1813f0fd5e7c3ea0884e3eaf3e91fe7757bc6553c`.
Versioning remained disabled (`null`). No official objects were written,
changed, or deleted. Test objects are retained.

One initial verifier attempt stopped after generating REES46 test output because
the verifier used an unsupported PyArrow schema method. No official objects
were written. The incomplete bucket `nutee-session-v2-test-20261126` and its
objects are intentionally retained; the corrected run used a separate bucket.

## Remaining limitation

MerRec Silver already trims/casts source IDs. GĐ2 does not modify that
normalization; `session_key` uses the values available in Gold and cannot
recover distinctions already lost in Silver.

GĐ2 status is **TESTED**, not **ACCEPTED**. No production Gold V2 was published;
user acceptance is still required before proceeding.
