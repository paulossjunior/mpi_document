from __future__ import annotations

import hashlib
import re
from pathlib import Path
from collections.abc import Iterator
from typing import Optional, Set

from document_etl.models import SourceDocument


DEFAULT_EXTENSIONS = {
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".tif",
    ".tiff",
    ".bmp",
    ".webp",
}


class LocalFolderSource:
    def __init__(
        self,
        source_dir: Path,
        extensions: Optional[Set[str]] = None,
        recursive: bool = False,
    ) -> None:
        self.source_dir = source_dir
        self.extensions = {ext.lower() for ext in (extensions or DEFAULT_EXTENSIONS)}
        self.recursive = recursive

    def iter_documents(self) -> Iterator[SourceDocument]:
        if not self.source_dir.exists():
            return

        pattern = "**/*" if self.recursive else "*"
        for path in sorted(self.source_dir.glob(pattern)):
            if not path.is_file() or path.suffix.lower() not in self.extensions:
                continue

            sha256 = self._sha256(path)
            document_id = self._document_id(path, sha256)
            stat = path.stat()
            yield SourceDocument(
                document_id=document_id,
                path=path,
                filename=path.name,
                extension=path.suffix.lower(),
                size_bytes=stat.st_size,
                sha256=sha256,
            )

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _document_id(path: Path, sha256: str) -> str:
        stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", path.stem).strip("._")
        return f"{stem or 'document'}-{sha256[:12]}"
