"""Compatibility entrypoint for the safe, two-dataset Gold V2 MinIO verifier."""

import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from test.common.verify_gold_v2_minio import main


if __name__ == "__main__":
    main()
