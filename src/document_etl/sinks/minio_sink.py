from __future__ import annotations

import mimetypes
import os
import re
from pathlib import Path
from typing import Iterable


class MinioSink:
    def __init__(
        self,
        endpoint: str | None = None,
        bucket_name: str | None = None,
        bucket_per_document: bool = False,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool | None = None,
    ) -> None:
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "document-etl")
        self.bucket_per_document = bucket_per_document
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.secure = secure if secure is not None else os.getenv("MINIO_SECURE", "false").lower() == "true"

    def write_document_dirs(self, document_dirs: Iterable[Path]) -> int:
        from minio import Minio

        client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

        uploaded = 0
        for document_dir in document_dirs:
            bucket_name = self._bucket_for_document(document_dir.name)
            self._ensure_bucket(client, bucket_name)
            uploaded += self._upload_document_dir(client, document_dir)
        return uploaded

    def _ensure_bucket(self, client: object, bucket_name: str) -> None:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

    def _upload_document_dir(self, client: object, document_dir: Path) -> int:
        uploaded = 0
        document_id = document_dir.name
        bucket_name = self._bucket_for_document(document_id)

        for file_path in sorted(document_dir.rglob("*")):
            if not file_path.is_file():
                continue

            relative_path = file_path.relative_to(document_dir)
            object_name = relative_path.as_posix() if self.bucket_per_document else f"{document_id}/{relative_path.as_posix()}"
            content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"

            client.fput_object(
                bucket_name,
                object_name,
                str(file_path),
                content_type=content_type,
            )
            uploaded += 1

        return uploaded

    def _bucket_for_document(self, document_id: str) -> str:
        if not self.bucket_per_document:
            return self.bucket_name

        suffix = self._sanitize_bucket_part(document_id)
        bucket_name = f"{self.bucket_name}-{suffix}"
        if len(bucket_name) <= 63:
            return bucket_name

        keep = 63 - len(self.bucket_name) - 1
        return f"{self.bucket_name}-{suffix[:keep].strip('-')}"

    @staticmethod
    def _sanitize_bucket_part(value: str) -> str:
        value = value.lower()
        value = re.sub(r"[^a-z0-9.-]+", "-", value)
        value = re.sub(r"-+", "-", value)
        value = value.strip(".-")
        return value or "document"
