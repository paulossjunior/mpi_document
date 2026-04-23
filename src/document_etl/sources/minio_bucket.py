from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator
from pathlib import Path

from document_etl.models import SourceDocument
from document_etl.sources.local_folder import DEFAULT_EXTENSIONS


class MinioBucketSource:
    def __init__(
        self,
        download_dir: Path,
        bucket_name: str,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = False,
        recursive: bool = True,
        source_prefix: str = "source/",
        processing_prefix: str = "processing/",
        failed_prefix: str = "failed/",
    ) -> None:
        self.download_dir = download_dir
        self.bucket_name = bucket_name
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.recursive = recursive
        self.source_prefix = self._normalize_prefix(source_prefix)
        self.processing_prefix = self._normalize_prefix(processing_prefix)
        self.failed_prefix = self._normalize_prefix(failed_prefix)

    def iter_documents(self) -> Iterator[SourceDocument]:
        client = self._client()

        self.download_dir.mkdir(parents=True, exist_ok=True)

        for obj in client.list_objects(self.bucket_name, prefix=self.source_prefix, recursive=self.recursive):
            object_name = obj.object_name
            path = Path(object_name)
            if path.suffix.lower() not in DEFAULT_EXTENSIONS:
                continue

            processing_name = self._move_object(client, object_name, self.processing_prefix)
            if processing_name is None:
                continue

            local_path = self.download_dir / processing_name
            local_path.parent.mkdir(parents=True, exist_ok=True)
            client.fget_object(self.bucket_name, processing_name, str(local_path))

            sha256 = self._sha256(local_path)
            document_id = self._document_id(local_path, sha256)
            stat = local_path.stat()
            yield SourceDocument(
                document_id=document_id,
                path=local_path,
                filename=Path(processing_name).name,
                extension=path.suffix.lower(),
                size_bytes=stat.st_size,
                sha256=sha256,
                source_object_name=processing_name,
            )

    def delete_document(self, document: SourceDocument) -> None:
        if not document.source_object_name:
            return

        client = self._client()
        client.remove_object(self.bucket_name, document.source_object_name)

    def mark_failed(self, document: SourceDocument) -> None:
        if not document.source_object_name:
            return

        client = self._client()
        self._move_existing_object(client, document.source_object_name, self.failed_prefix)

    def _client(self) -> object:
        from minio import Minio

        return Minio(
            self.endpoint or "localhost:9000",
            access_key=self.access_key or "minioadmin",
            secret_key=self.secret_key or "minioadmin",
            secure=self.secure,
        )

    def _move_object(self, client: object, object_name: str, target_prefix: str) -> str | None:
        destination = self._target_name(object_name, target_prefix)
        try:
            self._move_existing_object(client, object_name, target_prefix)
        except Exception:
            return None
        return destination

    def _move_existing_object(self, client: object, object_name: str, target_prefix: str) -> str:
        from minio.commonconfig import CopySource

        destination = self._target_name(object_name, target_prefix)
        client.copy_object(self.bucket_name, destination, CopySource(self.bucket_name, object_name))
        client.remove_object(self.bucket_name, object_name)
        return destination

    def _target_name(self, object_name: str, target_prefix: str) -> str:
        relative_name = object_name
        if self.source_prefix and object_name.startswith(self.source_prefix):
            relative_name = object_name[len(self.source_prefix) :]
        elif self.processing_prefix and object_name.startswith(self.processing_prefix):
            relative_name = object_name[len(self.processing_prefix) :]
        elif self.failed_prefix and object_name.startswith(self.failed_prefix):
            relative_name = object_name[len(self.failed_prefix) :]
        return f"{target_prefix}{relative_name}" if target_prefix else relative_name

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        prefix = prefix.strip("/")
        return f"{prefix}/" if prefix else ""

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
