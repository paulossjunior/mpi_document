from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from document_etl.sinks.folder_sink import FolderSink
from document_etl.sources.local_folder import LocalFolderSource
from document_etl.transforms.docling_transform import DoclingTransform

log = logging.getLogger(__name__)


class DocumentEtlFlow:
    def __init__(
        self,
        source_dir: Path = Path("data/source"),
        sink_dir: Path = Path("data/sink"),
        recursive: bool = False,
    ) -> None:
        self.source = LocalFolderSource(source_dir=source_dir, recursive=recursive)
        self.transform = DoclingTransform()
        self.sink = FolderSink(sink_dir=sink_dir)

    def run(self) -> List[Path]:
        output_paths: List[Path] = []
        for source_document in self.source.iter_documents():
            log.info("processing document_id=%s path=%s", source_document.document_id, source_document.path)
            artifacts = self.transform.transform(source_document)
            output_path = self.sink.write(artifacts)
            output_paths.append(output_path)
            log.info("wrote document_id=%s output=%s", source_document.document_id, output_path)
        return output_paths
