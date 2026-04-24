from __future__ import annotations

"""Docling-based transform stage for extracting structured document artifacts."""

from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import replace
import logging
import os
import time
from typing import Any, Optional

from document_etl.models import DocumentArtifacts, SourceDocument
from document_etl.transforms.extractors import (
    ArtifactStrategy,
    DoclingConverterFactory,
    ImageExtractionStrategy,
    TableStrategy,
    TextBlockStrategy,
)
from document_etl.transforms.extractors.document_value_adapter import DocumentValueAdapter

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformPolicy:
    """Runtime transform settings chosen for a specific source document."""

    name: str
    do_ocr: bool
    do_table_structure: bool
    generate_page_images: bool
    generate_picture_images: bool
    image_resolution_scale: float
    document_timeout: float
    chunk_size: int | None = None

    def cache_key(self) -> tuple[Any, ...]:
        return (
            self.name,
            self.do_ocr,
            self.do_table_structure,
            self.generate_page_images,
            self.generate_picture_images,
            self.image_resolution_scale,
            self.document_timeout,
            self.chunk_size,
        )

    def to_metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "do_ocr": self.do_ocr,
            "do_table_structure": self.do_table_structure,
            "generate_page_images": self.generate_page_images,
            "generate_picture_images": self.generate_picture_images,
            "image_resolution_scale": self.image_resolution_scale,
            "document_timeout": self.document_timeout,
            "chunk_size": self.chunk_size,
        }


class DoclingTransform:
    """Convert one source document into structured text, tables, images and metadata.

    Attributes:
        image_resolution_scale: Scale factor used when Docling renders images.
        document_timeout: Maximum time budget passed to Docling for one document.
        _converter: Lazily initialized Docling converter instance.
        _text_block_strategy: Strategy used to extract text blocks.
        _table_strategy: Strategy used to extract tables.
        _image_strategy: Strategy used to extract images.
        _converter_factory: Factory responsible for building Docling converters.
    """

    def __init__(
        self,
        image_resolution_scale: float = 2.0,
        document_timeout: float = 120,
        large_document_page_threshold: int | None = None,
        large_document_size_mb_threshold: int | None = None,
        large_document_timeout: float | None = None,
        large_document_page_chunk_size: int | None = None,
    ) -> None:
        self.image_resolution_scale = image_resolution_scale
        self.document_timeout = document_timeout
        self.large_document_page_threshold = large_document_page_threshold or int(
            os.getenv("LARGE_DOCUMENT_PAGE_THRESHOLD", "200")
        )
        self.large_document_size_mb_threshold = large_document_size_mb_threshold or int(
            os.getenv("LARGE_DOCUMENT_SIZE_MB_THRESHOLD", "20")
        )
        self.large_document_timeout = large_document_timeout or float(os.getenv("LARGE_DOCUMENT_TIMEOUT", "900"))
        self.large_document_page_chunk_size = large_document_page_chunk_size or int(
            os.getenv("LARGE_DOCUMENT_PAGE_CHUNK_SIZE", "25")
        )
        self._converters: dict[tuple[Any, ...], Any] = {}
        self._text_block_strategy: ArtifactStrategy = TextBlockStrategy()
        self._table_strategy: ArtifactStrategy = TableStrategy()
        self._image_strategy = ImageExtractionStrategy()
        self._converter_factory = DoclingConverterFactory()

    def transform(self, source: SourceDocument) -> DocumentArtifacts:
        """Run Docling conversion and normalize the result into project models."""
        policy = self._select_policy(source)
        started_at = time.perf_counter()
        log.info(
            "starting transform document_id=%s path=%s policy=%s page_count=%s size_bytes=%s",
            source.document_id,
            source.path,
            policy.name,
            source.page_count,
            source.size_bytes,
        )
        if policy.chunk_size and source.page_count:
            return self._transform_chunked(source=source, policy=policy, started_at=started_at)

        return self._transform_single(source=source, policy=policy, started_at=started_at)

    def _transform_single(
        self,
        *,
        source: SourceDocument,
        policy: TransformPolicy,
        started_at: float,
        page_range: tuple[int, int] | None = None,
    ) -> DocumentArtifacts:
        """Transform one full document or one specific page range."""
        try:
            convert_kwargs: dict[str, Any] = {"raises_on_error": False}
            if page_range is not None:
                convert_kwargs["page_range"] = page_range
            result = self.converter_for(policy).convert(source.path, **convert_kwargs)
            document = result.document
            elapsed = round(time.perf_counter() - started_at, 3)
            artifacts = DocumentArtifacts(
                source=source,
                status=str(getattr(result, "status", "success")),
                markdown=document.export_to_markdown(),
                text=DocumentValueAdapter.export_text(document),
                text_blocks=self._text_block_strategy.extract(document, source),
                tables=self._table_strategy.extract(document, source),
                images=self._image_strategy.extract(document, source),
                docling_json=document.export_to_dict(),
                processing_profile=policy.to_metadata(),
                diagnostics={
                    "elapsed_seconds": elapsed,
                    "page_count": source.page_count,
                    "size_mb": round(source.size_bytes / (1024 * 1024), 3),
                    "page_range": list(page_range) if page_range is not None else None,
                },
                errors=self._serialize_result_errors(result, page_range=page_range),
            )
            log.info(
                "finished transform document_id=%s status=%s policy=%s elapsed_seconds=%s text_blocks=%s tables=%s images=%s errors=%s",
                source.document_id,
                artifacts.status,
                policy.name,
                elapsed,
                len(artifacts.text_blocks),
                len(artifacts.tables),
                len(artifacts.images),
                len(artifacts.errors),
            )
            return artifacts
        except Exception as exc:
            elapsed = round(time.perf_counter() - started_at, 3)
            log.exception("transform failed document_id=%s path=%s", source.document_id, source.path)
            return DocumentArtifacts(
                source=source,
                status="failure",
                processing_profile=policy.to_metadata(),
                diagnostics={
                    "elapsed_seconds": elapsed,
                    "page_count": source.page_count,
                    "size_mb": round(source.size_bytes / (1024 * 1024), 3),
                    "page_range": list(page_range) if page_range is not None else None,
                },
                errors=[
                    {
                        "stage": "transform",
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "source": asdict(source),
                        "processing_profile": policy.to_metadata(),
                        "elapsed_seconds": elapsed,
                        "page_range": list(page_range) if page_range is not None else None,
                    }
                ],
            )

    def _transform_chunked(
        self,
        *,
        source: SourceDocument,
        policy: TransformPolicy,
        started_at: float,
    ) -> DocumentArtifacts:
        """Process large PDFs in page batches while keeping full extraction enabled."""
        assert source.page_count is not None
        assert policy.chunk_size is not None

        chunk_results: list[DocumentArtifacts] = []
        for start_page in range(1, source.page_count + 1, policy.chunk_size):
            end_page = min(start_page + policy.chunk_size - 1, source.page_count)
            log.info(
                "processing document chunk document_id=%s page_range=%s-%s chunk_size=%s",
                source.document_id,
                start_page,
                end_page,
                policy.chunk_size,
            )
            chunk_started_at = time.perf_counter()
            chunk_results.append(
                self._transform_single(
                    source=source,
                    policy=policy,
                    started_at=chunk_started_at,
                    page_range=(start_page, end_page),
                )
            )

        elapsed = round(time.perf_counter() - started_at, 3)
        merged = self._merge_chunk_results(
            source=source,
            policy=policy,
            chunk_results=chunk_results,
            elapsed=elapsed,
        )
        log.info(
            "finished chunked transform document_id=%s status=%s chunks=%s elapsed_seconds=%s text_blocks=%s tables=%s images=%s errors=%s",
            source.document_id,
            merged.status,
            len(chunk_results),
            elapsed,
            len(merged.text_blocks),
            len(merged.tables),
            len(merged.images),
            len(merged.errors),
        )
        return merged

    def converter_for(self, policy: TransformPolicy) -> Any:
        """Build and cache Docling converters per policy profile."""
        cache_key = policy.cache_key()
        if cache_key not in self._converters:
            log.info(
                "initializing Docling converter policy=%s image_resolution_scale=%s document_timeout=%s do_ocr=%s tables=%s page_images=%s picture_images=%s",
                policy.name,
                policy.image_resolution_scale,
                policy.document_timeout,
                policy.do_ocr,
                policy.do_table_structure,
                policy.generate_page_images,
                policy.generate_picture_images,
            )
            self._converters[cache_key] = self._build_converter(policy)
        return self._converters[cache_key]

    def _build_converter(self, policy: TransformPolicy) -> Any:
        """Create the configured Docling converter for PDFs and images."""
        return self._converter_factory.create(
            image_resolution_scale=policy.image_resolution_scale,
            document_timeout=policy.document_timeout,
            do_ocr=policy.do_ocr,
            do_table_structure=policy.do_table_structure,
            generate_page_images=policy.generate_page_images,
            generate_picture_images=policy.generate_picture_images,
        )

    def _select_policy(self, source: SourceDocument) -> TransformPolicy:
        """Choose a chunked full-extraction profile automatically for very large PDFs."""
        is_large_pdf = source.extension == ".pdf" and (
            (source.page_count is not None and source.page_count >= self.large_document_page_threshold)
            or (source.size_bytes / (1024 * 1024)) >= self.large_document_size_mb_threshold
        )

        if is_large_pdf:
            return TransformPolicy(
                name="large_document_chunked",
                do_ocr=True,
                do_table_structure=True,
                generate_page_images=True,
                generate_picture_images=True,
                image_resolution_scale=self.image_resolution_scale,
                document_timeout=self.large_document_timeout,
                chunk_size=self.large_document_page_chunk_size,
            )

        return TransformPolicy(
            name="default",
            do_ocr=True,
            do_table_structure=True,
            generate_page_images=True,
            generate_picture_images=True,
            image_resolution_scale=self.image_resolution_scale,
            document_timeout=self.document_timeout,
        )

    def _merge_chunk_results(
        self,
        *,
        source: SourceDocument,
        policy: TransformPolicy,
        chunk_results: list[DocumentArtifacts],
        elapsed: float,
    ) -> DocumentArtifacts:
        """Merge page-range chunk outputs into one sink-ready artifact set."""
        markdown_parts: list[str] = []
        text_parts: list[str] = []
        text_blocks = []
        tables = []
        images = []
        errors = []
        chunk_metadata = []
        statuses: list[str] = []
        image_filenames: set[str] = set()

        text_index = 1
        table_index = 1
        image_index = 1
        picture_index = 1

        for chunk in chunk_results:
            page_range = chunk.diagnostics.get("page_range")
            statuses.append(chunk.status)
            if chunk.markdown:
                markdown_parts.append(chunk.markdown)
            if chunk.text:
                text_parts.append(chunk.text)

            for block in chunk.text_blocks:
                text_blocks.append(replace(block, index=text_index))
                text_index += 1

            for table in chunk.tables:
                tables.append(replace(table, index=table_index))
                table_index += 1

            for image in chunk.images:
                filename = image.filename
                if image.kind == "picture":
                    filename = f"picture_{picture_index:03d}.png"
                    picture_index += 1
                elif filename in image_filenames:
                    filename = f"image_{image_index:03d}.png"

                image_filenames.add(filename)
                images.append(replace(image, index=image_index, filename=filename))
                image_index += 1

            errors.extend(chunk.errors)
            chunk_metadata.append(
                {
                    "page_range": page_range,
                    "status": chunk.status,
                    "counts": {
                        "text_blocks": len(chunk.text_blocks),
                        "tables": len(chunk.tables),
                        "images": len(chunk.images),
                        "errors": len(chunk.errors),
                    },
                    "diagnostics": chunk.diagnostics,
                }
            )

        return DocumentArtifacts(
            source=source,
            status=self._merge_statuses(statuses, errors),
            markdown="\n\n".join(part for part in markdown_parts if part),
            text="\n\n".join(part for part in text_parts if part),
            text_blocks=text_blocks,
            tables=tables,
            images=images,
            docling_json={
                "schema_name": "chunked_docling_export",
                "name": source.filename,
                "origin": {
                    "filename": source.filename,
                    "mimetype": "application/pdf" if source.extension == ".pdf" else None,
                },
                "page_count": source.page_count,
                "chunks": chunk_metadata,
            },
            errors=errors,
            processing_profile=policy.to_metadata(),
            diagnostics={
                "elapsed_seconds": elapsed,
                "page_count": source.page_count,
                "size_mb": round(source.size_bytes / (1024 * 1024), 3),
                "chunk_count": len(chunk_results),
                "chunk_ranges": [chunk.diagnostics.get("page_range") for chunk in chunk_results],
            },
        )

    @staticmethod
    def _merge_statuses(statuses: list[str], errors: list[dict[str, Any]]) -> str:
        """Collapse chunk statuses into one high-level document status."""
        normalized = [status.lower() for status in statuses if status]
        if errors:
            return "ConversionStatus.PARTIAL_SUCCESS"
        if normalized and all(status.endswith("success") and not status.startswith("conversionstatus.partial") for status in normalized):
            return "ConversionStatus.SUCCESS"
        if any(status.endswith("success") for status in normalized):
            return "ConversionStatus.PARTIAL_SUCCESS"
        return statuses[0] if statuses else "failure"

    @staticmethod
    def _serialize_result_errors(result: Any, *, page_range: tuple[int, int] | None) -> list[dict[str, Any]]:
        """Serialize Docling conversion errors into project-friendly dictionaries."""
        serialized = []
        for error in getattr(result, "errors", []) or []:
            serialized.append(
                {
                    "stage": "transform",
                    "type": type(error).__name__,
                    "message": getattr(error, "error_message", str(error)),
                    "details": error.model_dump(mode="json", exclude_none=True)
                    if hasattr(error, "model_dump")
                    else str(error),
                    "page_range": list(page_range) if page_range is not None else None,
                }
            )
        return serialized
