from __future__ import annotations

import mimetypes
import logging
import os
from pathlib import Path
from typing import Iterable

log = logging.getLogger(__name__)


class MinioSink:
    """Upload document sink directories into a shared MinIO bucket.

    Processed documents are isolated by object prefix, not by bucket name:
    ``<root_prefix><document_id>/...``.

    Attributes:
        endpoint: MinIO endpoint used for uploads.
        bucket_name: Shared sink bucket that receives all processed documents.
        root_prefix: Optional namespace prefix added ahead of ``document_id``.
        access_key: MinIO access key used for authentication.
        secret_key: MinIO secret key used for authentication.
        secure: Whether HTTPS should be used for MinIO connections.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        bucket_name: str | None = None,
        root_prefix: str = "",
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool | None = None,
    ) -> None:
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.bucket_name = bucket_name or os.getenv("MINIO_SINK_BUCKET") or os.getenv("MINIO_BUCKET", "sink")
        self.root_prefix = self._normalize_prefix(root_prefix or os.getenv("MINIO_SINK_PREFIX", ""))
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.secure = secure if secure is not None else os.getenv("MINIO_SECURE", "false").lower() == "true"

    def write_document_dirs(self, document_dirs: Iterable[Path]) -> int:
        """Upload one or more document directories and return the object count."""
        from minio import Minio

        client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

        uploaded = 0
        for document_dir in document_dirs:
            log.info("uploading document directory to MinIO bucket=%s path=%s", self.bucket_name, document_dir)
            self._ensure_bucket(client, self.bucket_name)
            uploaded += self._upload_document_dir(client, document_dir)
        log.info("finished MinIO upload bucket=%s uploaded_objects=%s", self.bucket_name, uploaded)
        return uploaded

    def _ensure_bucket(self, client: object, bucket_name: str) -> None:
        if not client.bucket_exists(bucket_name):
            log.info("creating sink bucket bucket=%s", bucket_name)
            client.make_bucket(bucket_name)

    def _upload_document_dir(self, client: object, document_dir: Path) -> int:
        """Persist a single document tree under the configured bucket/prefix."""
        uploaded = 0
        document_id = document_dir.name

        for file_path in sorted(document_dir.rglob("*")):
            if not file_path.is_file():
                continue

            relative_path = file_path.relative_to(document_dir)
            object_name = f"{self.root_prefix}{document_id}/{relative_path.as_posix()}"
            content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
            log.debug("uploading object bucket=%s object_name=%s", self.bucket_name, object_name)

            client.fput_object(
                self.bucket_name,
                object_name,
                str(file_path),
                content_type=content_type,
            )
            uploaded += 1

        return uploaded

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        """Normalize prefixes so object paths can be joined safely."""
        prefix = prefix.strip("/")
        return f"{prefix}/" if prefix else ""
