from contextlib import contextmanager
import csv
import gzip
from hashlib import file_digest
from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from urllib.parse import unquote, urlsplit
from urllib.request import urlopen

import pyarrow.parquet as pq


@contextmanager
def extract(source: str, base_dir: Path):
    """Copy a local file or an HTTP(S) file into a temporary workspace."""
    location = urlsplit(source)
    is_remote = location.scheme.lower() in {"http", "https"}
    if is_remote:
        name = Path(unquote(location.path)).name
    else:
        local_file = base_dir / source
        name = local_file.name

    if name in {"", ".", ".."}:
        raise ValueError(f"Source must point to a file: {source}")

    with TemporaryDirectory(prefix="nutee-source-") as temp_dir:
        path = Path(temp_dir) / name
        stream = urlopen(source, timeout=60) if is_remote else local_file.open("rb")
        with stream, path.open("wb") as destination:
            copyfileobj(stream, destination, length=1024 * 1024)
        with path.open("rb") as extracted:
            checksum = file_digest(extracted, "sha256").hexdigest()
        yield path, checksum


@contextmanager
def read_rows(source: Path, batch_size: int = 25_000):
    """Read CSV/Gzip or Parquet in file order, closing handles on Windows."""
    if source.suffix.lower() == ".parquet":
        with pq.ParquetFile(source) as parquet:
            rows = (row for batch in parquet.iter_batches(batch_size=batch_size) for row in batch.to_pylist())
            yield parquet.schema_arrow.names, enumerate(rows, start=1)
    else:
        opener = gzip.open if source.suffix.lower() == ".gz" else open
        with opener(source, "rt", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            yield reader.fieldnames or [], ((reader.line_num, row) for row in reader)
