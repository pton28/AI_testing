# GĐ7 – Reproducibility & Final Acceptance

**Status: PARTIAL — NOT TESTED / NOT ACCEPTED.** The user authorized a
shortened run to preserve the outputs. Do not treat this report or the MerRec
manifest as final acceptance or publication approval.

## Results

| Output / criterion | REES46 expected / actual | Status | MerRec expected / actual | Status |
|---|---:|---|---:|---|
| Bronze events | 885,129 / 885,129 | PASS | 605,269 / 605,269 | PASS |
| Silver events | 885,129 / 885,129 | PASS | 605,269 / 605,269 | PASS |
| Gold V3 events | 885,129 / 885,129 | PASS | 515,601 / 515,601 | PASS |
| Unique session keys | 490,633 / 490,633 | PASS | 112,859 / 112,859 | PASS |
| Shared session keys | 0 / 0 | PASS | 0 / 0 | PASS |
| Ambiguous events | 7,939 / 7,939 | PASS | 393 / 393 | PASS |
| Product Dimension rows | 53,453 / 53,453 | PASS | 431,734 / 431,734 | PASS |
| ALS interaction rows | 556,690 / 556,690 | PASS | 429,812 / 429,812 | PASS |
| ALS total interactions | 885,129 / 885,129 | PASS | 515,601 / 515,601 | PASS |
| Session sequences | 490,633 / 490,633 | PASS | 112,859 / 112,859 | PASS |
| Sequence events | 884,964 / 884,964 | PASS | 515,601 / 515,601 | PASS |
| Product coverage | 100% / 100% | PASS | 100% / 100% | PASS |
| Two-run reproducibility | Two runs completed | PASS | Second run stopped | **NOT TESTED** |

Event distribution:

- REES46: `product_view=793,748`, `add_to_cart=54,035`, `purchase=37,346`.
- MerRec: `product_view=504,756`, `add_to_cart=10,168`, `purchase=677`.

REES46 completed both runs. Its manifest reports `PASS` for that dataset.
MerRec outputs were created in one run and checked against accepted G4–G6
logical baselines: Silver and Gold V3 rows matched G4; Product Dimension
matched G5; ALS interactions and session sequences matched G6. Schema and
row-count checks also passed. The second MerRec rebuild was stopped during
Product Dimension generation; only its Silver, Gold V3 and Product Dimension
files existed locally at that point. The first-pass outputs for all stages
remain in MinIO; therefore G7 overall is not reproducible/accepted yet.
For REES46, all five Parquet outputs were logically equal between runs and
their physical SHA-256 values were also equal. Its rebuilt Silver remains
logically identical to official Silver despite a different physical SHA-256.

## Tests and verification

- Unit and regression tests: **PASS**, 39 tests.
- Verifier compile and `--help`: **PASS**.
- G7 MinIO end-to-end: **PARTIAL**. REES46 two-pass complete; MerRec one-pass
  outputs retained and validated against accepted baselines.
- MerRec second-run reproducibility: **NOT TESTED**.
- No official publication was performed. No official or accepted baseline
  writes were issued. The final official bucket inventory count was 16 and
  bucket versioning reported `None`; a complete before/after inventory
  comparison was not captured after the shortened run.

Parquet SHA-256 differences are not automatically treated as content changes.
For example, the existing official REES46 Silver and the rebuilt Silver have
different byte-level SHA-256 values but identical schema and logical rows.

## Test bucket outputs

Bucket: `nutee-final-acceptance-g7-test-20261126-181223`

Both datasets have Bronze, Silver, Gold V3, Product Dimension, ALS interactions
and session-sequence objects under their existing versioned keys:

```text
bronze/<dataset>/<source_version>/<source-file>
silver/<dataset>/<adapter>/<source_version>/events.parquet
gold/v3/<dataset>/<source_version>/events.parquet
gold/v3/<dataset>/<source_version>/products.parquet
gold/v3/<dataset>/<source_version>/model_ready/als_interactions.parquet
gold/v3/<dataset>/<source_version>/model_ready/session_sequences.parquet
manifests/<dataset>/<source_version>/manifest.json
```

REES46 manifest:
`manifests/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/manifest.json`

MerRec manifest:
`manifests/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/manifest.json`
It is marked `NOT_TESTED` for full G7 acceptance and reproducibility.

## Publication plan — pending approval

- Keep official Bronze objects unchanged.
- Keep official REES46 Silver. The rebuilt file is logically identical; its
  physical SHA-256 differs.
- Keep the existing official MerRec Silver key unchanged. Its content differs
  from the restored 605,269-row Silver; if approved later, publish the tested
  result under
  `silver/merrec/merrec_events/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/event-preservation-g3/events.parquet`.
- Gold V3, Product Dimension and model-ready objects are new candidate keys;
  publish only after full G7 acceptance and explicit approval.
- Preserve Gold V1/V2 and accepted G2–G6 baselines. No bucket versioning or
  official objects were intentionally changed.

## Remaining work

Run MerRec through a second complete rebuild, finish logical/schema and
official-inventory comparisons, then update the MerRec manifest to full
acceptance only if all checks pass. No AI training or train/test split was run.
