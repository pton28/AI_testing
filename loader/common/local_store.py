"""Publish completed pipeline files atomically under the local data directory."""

import os
from pathlib import Path
from shutil import copyfileobj
from tempfile import NamedTemporaryFile


class LocalLoader:
    def __init__(self, root: Path):
        self.root = root.resolve()

    def path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root) or path == self.root:
            raise ValueError("Storage key must point to a file inside data/")
        return path

    def location(self, key: str) -> str:
        return str(self.path(key))

    def load(self, key: str, path: Path, content_type: str) -> None:
        destination = self.path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            # Same directory/volume enables atomic replacement on Windows too.
            with NamedTemporaryFile(dir=destination.parent, prefix=".nutee-", suffix=".tmp", delete=False) as output:
                temporary = Path(output.name)
                with path.open("rb") as source:
                    copyfileobj(source, output, length=1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, destination)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
