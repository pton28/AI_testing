from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from minio import Minio


@contextmanager
def extract_object(client: Minio, bucket: str, key: str):
    """Download a stored Parquet object into a temporary workspace."""
    with TemporaryDirectory(prefix="nutee-minio-") as directory:
        path = Path(directory) / "events.parquet"
        client.fget_object(bucket, key, str(path))
        yield path
