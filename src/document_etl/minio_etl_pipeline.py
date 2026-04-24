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
    """Run the end-to-end MinIO-first ETL flow.

    Source objects are claimed from MinIO, transformed locally with Docling,
    materialized with ``FolderSink`` and finally uploaded to the sink bucket.

    Attributes:
        source_bucket: Bucket that contains the input objects to be processed.
        endpoint: MinIO endpoint used by both source and sink operations.
        sink_bucket: Bucket that receives the structured sink output.
        access_key: MinIO access key used for authentication.
        secret_key: MinIO secret key used for authentication.
        secure: Whether HTTPS should be used for MinIO connections.
        source_prefix: Optional prefix that contains new objects awaiting processing.
        processing_prefix: Prefix used to claim objects before downloading.
        failed_prefix: Prefix used to store objects that failed processing.
        recovery_timeout_seconds: Age threshold used to retry orphaned ``processing/`` objects.
        sink_prefix: Optional namespace prefix inside the sink bucket.
        transform: Transform stage that converts a source document into artifacts.
    """

    def __init__(
        self,
        source_bucket: str,
        endpoint: str | None = None,
        sink_bucket: str = "sink",
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = False,
        source_prefix: str = "",
        processing_prefix: str = "processing/",
        failed_prefix: str = "failed/",
        recovery_timeout_seconds: float = 300.0,
        sink_prefix: str = "",
    ) -> None:
        self.source_bucket = source_bucket
        self.endpoint = endpoint
        self.sink_bucket = sink_bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.source_prefix = source_prefix
        self.processing_prefix = processing_prefix
        self.failed_prefix = failed_prefix
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.sink_prefix = sink_prefix
        self.transform = DoclingTransform()

    def run(self) -> int:
        """Process all available documents and return the uploaded object count."""
        log.info(
            "starting ETL run source_bucket=%s sink_bucket=%s source_prefix=%s sink_prefix=%s",
            self.source_bucket,
            self.sink_bucket,
            self.source_prefix,
            self.sink_prefix,
        )
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
                recovery_timeout_seconds=self.recovery_timeout_seconds,
            )
            folder_sink = FolderSink(sink_dir=Path(sink_tmp))
            minio_sink = MinioSink(
                endpoint=self.endpoint,
                bucket_name=self.sink_bucket,
                root_prefix=self.sink_prefix,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )

            uploaded_count = 0
            processed_documents = 0
            for source_document in source.iter_documents():
                processed_documents += 1
                log.info("processing source bucket document filename=%s", source_document.filename)
                artifacts = self.transform.transform(source_document)
                document_dir = folder_sink.write(artifacts)
                log.info("wrote transformed document to temp sink path=%s", document_dir)
                # Only successful conversions leave the processing area and
                # become the canonical sink representation.
                if artifacts.status.lower().endswith("success") and not artifacts.errors:
                    uploaded_count += minio_sink.write_document_dirs([document_dir])
                    source.delete_document(source_document)
                    log.info("deleted processed source object object_name=%s", source_document.source_object_name)
                else:
                    # Failed objects are preserved for inspection outside the
                    # hot processing path.
                    source.mark_failed(source_document)
                    log.warning("moved failed source object to failed prefix object_name=%s", source_document.source_object_name)

            log.info(
                "finished ETL run processed_documents=%s uploaded_objects=%s",
                processed_documents,
                uploaded_count,
            )
            return uploaded_count
