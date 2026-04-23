from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from document_etl.sinks.folder_sink import FolderSink
from document_etl.sinks.minio_sink import MinioSink
from document_etl.sources.minio_bucket import MinioBucketSource
from document_etl.transforms.docling_transform import DoclingTransform

log = logging.getLogger(__name__)


class MinioDocumentEtlFlow:
    def __init__(
        self,
        source_bucket: str,
        endpoint: str | None = None,
        bucket_name: str = "document-etl",
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = False,
        source_prefix: str = "source/",
        processing_prefix: str = "processing/",
        failed_prefix: str = "failed/",
    ) -> None:
        self.source_bucket = source_bucket
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.source_prefix = source_prefix
        self.processing_prefix = processing_prefix
        self.failed_prefix = failed_prefix
        self.transform = DoclingTransform()

    def run(self) -> int:
        with tempfile.TemporaryDirectory(prefix="document-etl-source-") as source_tmp, tempfile.TemporaryDirectory(
            prefix="document-etl-sink-"
        ) as sink_tmp:
            source = MinioBucketSource(
                download_dir=Path(source_tmp),
                bucket_name=self.source_bucket,
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                source_prefix=self.source_prefix,
                processing_prefix=self.processing_prefix,
                failed_prefix=self.failed_prefix,
            )
            folder_sink = FolderSink(sink_dir=Path(sink_tmp))
            minio_sink = MinioSink(
                endpoint=self.endpoint,
                bucket_name=self.bucket_name,
                bucket_per_document=True,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )

            uploaded_count = 0
            for source_document in source.iter_documents():
                log.info("processing source bucket document filename=%s", source_document.filename)
                artifacts = self.transform.transform(source_document)
                document_dir = folder_sink.write(artifacts)
                log.info("wrote transformed document to temp sink path=%s", document_dir)
                if artifacts.status.lower().endswith("success") and not artifacts.errors:
                    uploaded_count += minio_sink.write_document_dirs([document_dir])
                    source.delete_document(source_document)
                    log.info("deleted processed source object object_name=%s", source_document.source_object_name)
                else:
                    source.mark_failed(source_document)
                    log.warning("moved failed source object to failed prefix object_name=%s", source_document.source_object_name)

            return uploaded_count
