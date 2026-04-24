from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from document_etl.models import SourceDocument

log = logging.getLogger(__name__)


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


class MinioBucketSource:
    """Read source objects from MinIO using a claim-before-download workflow.

    Attributes:
        download_dir: Local temporary directory where claimed objects are downloaded.
        bucket_name: MinIO bucket that holds the source objects.
        endpoint: MinIO endpoint used to create the client.
        access_key: MinIO access key used for authentication.
        secret_key: MinIO secret key used for authentication.
        secure: Whether HTTPS should be used for MinIO connections.
        recursive: Whether source object listing should recurse through prefixes.
        source_prefix: Optional prefix containing new objects ready for processing.
        processing_prefix: Prefix used when an object is claimed by a worker.
        failed_prefix: Prefix used to retain failed objects for inspection.
        recovery_timeout_seconds: Age threshold used to recover orphaned objects from ``processing/``.
    """

    def __init__(
        self,
        download_dir: Path,
        bucket_name: str,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = False,
        recursive: bool = True,
        source_prefix: str = "",
        processing_prefix: str = "processing/",
        failed_prefix: str = "failed/",
        recovery_timeout_seconds: float = 300.0,
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
        self.recovery_timeout_seconds = recovery_timeout_seconds

    def iter_documents(self) -> Iterator[SourceDocument]:
        """Yield downloaded documents after claiming new or recovering orphaned objects."""
        client = self._client()

        self.download_dir.mkdir(parents=True, exist_ok=True)
        yield from self._iter_recoverable_processing_documents(client)

        log.info(
            "listing source objects bucket=%s source_prefix=%s recursive=%s",
            self.bucket_name,
            self.source_prefix,
            self.recursive,
        )

        for obj in client.list_objects(self.bucket_name, prefix=self.source_prefix, recursive=self.recursive):
            object_name = obj.object_name
            if self._is_managed_object(object_name):
                log.debug("skipping managed object object_name=%s", object_name)
                continue

            path = Path(object_name)
            if path.suffix.lower() not in DEFAULT_EXTENSIONS:
                log.debug("skipping unsupported object object_name=%s", object_name)
                continue

            processing_name = self._move_object(client, object_name, self.processing_prefix)
            if processing_name is None:
                log.debug("skipping object because claim failed object_name=%s", object_name)
                continue

            yield self._download_document(client, processing_name)

    def _iter_recoverable_processing_documents(self, client: object) -> Iterator[SourceDocument]:
        """Yield orphaned processing objects that are older than the recovery timeout."""
        if not self.processing_prefix:
            return

        log.info(
            "checking processing prefix for recoverable objects bucket=%s processing_prefix=%s recovery_timeout_seconds=%s",
            self.bucket_name,
            self.processing_prefix,
            self.recovery_timeout_seconds,
        )
        now = datetime.now(timezone.utc)
        for obj in client.list_objects(self.bucket_name, prefix=self.processing_prefix, recursive=self.recursive):
            object_name = obj.object_name
            if Path(object_name).suffix.lower() not in DEFAULT_EXTENSIONS:
                continue

            last_modified = obj.last_modified
            if last_modified is None:
                log.warning("recovering processing object without last_modified object_name=%s", object_name)
                yield self._download_document(client, object_name)
                continue

            age_seconds = (now - last_modified).total_seconds()
            if age_seconds < self.recovery_timeout_seconds:
                log.debug(
                    "skipping fresh processing object object_name=%s age_seconds=%.2f recovery_timeout_seconds=%.2f",
                    object_name,
                    age_seconds,
                    self.recovery_timeout_seconds,
                )
                continue

            log.warning(
                "recovering orphaned processing object object_name=%s age_seconds=%.2f recovery_timeout_seconds=%.2f",
                object_name,
                age_seconds,
                self.recovery_timeout_seconds,
            )
            yield self._download_document(client, object_name)

    def _download_document(self, client: object, object_name: str) -> SourceDocument:
        """Download an already-claimed object and build the normalized source document."""
        local_path = self.download_dir / object_name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        log.info("downloading claimed object object_name=%s local_path=%s", object_name, local_path)
        client.fget_object(self.bucket_name, object_name, str(local_path))

        sha256 = self._sha256(local_path)
        document_id = self._document_id(local_path, sha256)
        stat = local_path.stat()
        page_count = self._page_count(local_path)
        log.info(
            "prepared source document document_id=%s filename=%s size_bytes=%s page_count=%s",
            document_id,
            Path(object_name).name,
            stat.st_size,
            page_count,
        )
        return SourceDocument(
            document_id=document_id,
            path=local_path,
            filename=Path(object_name).name,
            extension=local_path.suffix.lower(),
            size_bytes=stat.st_size,
            sha256=sha256,
            page_count=page_count,
            source_object_name=object_name,
        )

    def delete_document(self, document: SourceDocument) -> None:
        """Remove a successfully processed object from the processing prefix."""
        if not document.source_object_name:
            return

        client = self._client()
        log.info("removing processed object object_name=%s", document.source_object_name)
        client.remove_object(self.bucket_name, document.source_object_name)

    def mark_failed(self, document: SourceDocument) -> None:
        """Move a claimed object into the failed prefix for later inspection."""
        if not document.source_object_name:
            return

        client = self._client()
        log.warning("moving failed object to failed prefix object_name=%s", document.source_object_name)
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
        """Try to move an object and return ``None`` when the claim fails."""
        destination = self._target_name(object_name, target_prefix)
        try:
            self._move_existing_object(client, object_name, target_prefix)
        except Exception:
            log.debug("failed to claim object object_name=%s target_prefix=%s", object_name, target_prefix, exc_info=True)
            return None
        log.info("claimed object source=%s destination=%s", object_name, destination)
        return destination

    def _move_existing_object(self, client: object, object_name: str, target_prefix: str) -> str:
        """Implement MinIO rename semantics via copy-then-delete."""
        from minio.commonconfig import CopySource

        destination = self._target_name(object_name, target_prefix)
        log.debug("moving object source=%s destination=%s", object_name, destination)
        client.copy_object(self.bucket_name, destination, CopySource(self.bucket_name, object_name))
        client.remove_object(self.bucket_name, object_name)
        return destination

    def _target_name(self, object_name: str, target_prefix: str) -> str:
        """Swap only the state prefix while preserving the relative object path."""
        relative_name = object_name
        if self.source_prefix and object_name.startswith(self.source_prefix):
            relative_name = object_name[len(self.source_prefix) :]
        elif self.processing_prefix and object_name.startswith(self.processing_prefix):
            relative_name = object_name[len(self.processing_prefix) :]
        elif self.failed_prefix and object_name.startswith(self.failed_prefix):
            relative_name = object_name[len(self.failed_prefix) :]
        return f"{target_prefix}{relative_name}" if target_prefix else relative_name

    def _is_managed_object(self, object_name: str) -> bool:
        """Exclude state prefixes that belong to the worker itself."""
        return (
            (self.processing_prefix and object_name.startswith(self.processing_prefix))
            or (self.failed_prefix and object_name.startswith(self.failed_prefix))
        )

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        """Normalize prefixes so source state transitions stay predictable."""
        prefix = prefix.strip("/")
        return f"{prefix}/" if prefix else ""

    @staticmethod
    def _sha256(path: Path) -> str:
        """Hash the downloaded file so document ids remain deterministic."""
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _document_id(path: Path, sha256: str) -> str:
        """Build a path-safe identifier from the source filename and hash prefix."""
        stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", path.stem).strip("._")
        return f"{stem or 'document'}-{sha256[:12]}"

    @staticmethod
    def _page_count(path: Path) -> int | None:
        """Read the page count for PDFs so transforms can choose lighter profiles."""
        if path.suffix.lower() != ".pdf":
            return None
        try:
            from pypdfium2 import PdfDocument

            return len(PdfDocument(str(path)))
        except Exception:
            log.warning("failed to read pdf page count path=%s", path, exc_info=True)
            return None
