from __future__ import annotations

import logging
from pathlib import Path

from document_etl.sinks.minio_sink import MinioSink
from document_etl.sources.transformed_folder import TransformedFolderSource

log = logging.getLogger(__name__)


class SinkToMinioFlow:
    def __init__(
        self,
        sink_dir: Path = Path("data/sink"),
        endpoint: str | None = None,
        bucket_name: str = "document-etl",
        bucket_per_document: bool = False,
        secure: bool = False,
    ) -> None:
        self.source = TransformedFolderSource(sink_dir=sink_dir)
        self.sink = MinioSink(
            endpoint=endpoint,
            bucket_name=bucket_name,
            bucket_per_document=bucket_per_document,
            secure=secure,
        )

    def run(self) -> int:
        document_dirs = list(self.source.iter_document_dirs())
        for document_dir in document_dirs:
            log.info("uploading document directory to MinIO directory=%s", document_dir)
        return self.sink.write_document_dirs(document_dirs)
