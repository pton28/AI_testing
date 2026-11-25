from pathlib import Path

from minio import Minio
from minio.error import S3Error


class MinioLoader:
    def __init__(self, client: Minio, bucket: str):
        self.client = client
        self.bucket = bucket

    def ensure_bucket(self) -> None:
        if self.client.bucket_exists(self.bucket):
            return

        try:
            self.client.make_bucket(self.bucket)
        except S3Error as error:
            if error.code != "BucketAlreadyOwnedByYou":
                raise

    def load(self, key: str, path: Path, content_type: str) -> None:
        self.client.fput_object(
            self.bucket,
            key,
            str(path),
            content_type=content_type,
        )

    def location(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"
