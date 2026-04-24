from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class SourceDocument:
    """Normalized source input ready to be transformed.

    Attributes:
        document_id: Stable identifier used across sink paths and metadata.
        path: Local filesystem path to the claimed source file.
        filename: Original file name without bucket prefixes.
        extension: Lowercase file extension including the leading dot.
        size_bytes: File size in bytes after download.
        sha256: Content hash used to make the document id collision-resistant.
        page_count: Optional page count for paginated formats such as PDFs.
        source_object_name: Object name currently associated with the source in MinIO.
    """

    document_id: str
    path: Path
    filename: str
    extension: str
    size_bytes: int
    sha256: str
    page_count: Optional[int] = None
    source_object_name: Optional[str] = None


@dataclass
class TextBlock:
    """Structured text block extracted from the document body.

    Attributes:
        index: Stable order of the block in the extracted sequence.
        label: Optional semantic label reported by Docling.
        text: Plain text content for the block.
        page_no: Optional page number associated with the block.
        self_ref: Optional Docling self reference for traceability.
        provenance: Serialized provenance metadata for the block.
    """

    index: int
    label: Optional[str]
    text: str
    page_no: Optional[int] = None
    self_ref: Optional[str] = None
    provenance: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TableArtifact:
    """Structured table export in one or more serializable formats.

    Attributes:
        index: Stable order of the table inside the document.
        markdown: Markdown rendering of the table.
        html: HTML rendering of the table.
        dataframe: Optional dataframe representation when available.
        page_no: Optional page number where the table was found.
        self_ref: Optional Docling self reference for the table.
        provenance: Serialized provenance metadata for the table.
    """

    index: int
    markdown: str
    html: str
    dataframe: Optional[Any] = None
    page_no: Optional[int] = None
    self_ref: Optional[str] = None
    provenance: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ImageArtifact:
    """Image artifact extracted from a page, figure, or original source image.

    Attributes:
        index: Stable order of the image inside the sink.
        kind: Origin of the image, such as ``page``, ``picture`` or ``source``.
        filename: Output file name used by the sink writer.
        image: In-memory image object ready to be written as PNG.
        page_no: Optional page number associated with the image.
        self_ref: Optional Docling self reference for traceability.
        provenance: Serialized provenance metadata for the image.
    """

    index: int
    kind: str
    filename: str
    image: Any
    page_no: Optional[int] = None
    self_ref: Optional[str] = None
    provenance: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DocumentArtifacts:
    """Aggregate result produced by the transform stage for one document.

    Attributes:
        source: Original source document that generated the artifacts.
        status: High-level conversion status reported by the transform stage.
        markdown: Whole-document markdown export.
        text: Whole-document plain text export.
        text_blocks: Extracted text blocks in reading order.
        tables: Extracted tables with multiple serializations.
        images: Extracted page, picture or fallback source images.
        docling_json: Raw Docling JSON-compatible export for the document.
        errors: Structured errors collected during transformation.
        processing_profile: Runtime policy used by the transform for this document.
        diagnostics: Timing and transform diagnostics captured during processing.
    """

    source: SourceDocument
    status: str
    markdown: str = ""
    text: str = ""
    text_blocks: List[TextBlock] = field(default_factory=list)
    tables: List[TableArtifact] = field(default_factory=list)
    images: List[ImageArtifact] = field(default_factory=list)
    docling_json: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    processing_profile: Dict[str, Any] = field(default_factory=dict)
    diagnostics: Dict[str, Any] = field(default_factory=dict)
