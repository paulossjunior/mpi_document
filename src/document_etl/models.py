from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class SourceDocument:
    document_id: str
    path: Path
    filename: str
    extension: str
    size_bytes: int
    sha256: str


@dataclass
class TextBlock:
    index: int
    label: Optional[str]
    text: str
    page_no: Optional[int] = None
    self_ref: Optional[str] = None
    provenance: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TableArtifact:
    index: int
    markdown: str
    html: str
    dataframe: Optional[Any] = None
    page_no: Optional[int] = None
    self_ref: Optional[str] = None
    provenance: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ImageArtifact:
    index: int
    kind: str
    filename: str
    image: Any
    page_no: Optional[int] = None
    self_ref: Optional[str] = None
    provenance: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DocumentArtifacts:
    source: SourceDocument
    status: str
    markdown: str = ""
    text: str = ""
    text_blocks: List[TextBlock] = field(default_factory=list)
    tables: List[TableArtifact] = field(default_factory=list)
    images: List[ImageArtifact] = field(default_factory=list)
    docling_json: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
