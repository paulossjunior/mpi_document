from __future__ import annotations

"""Protocol definitions for transform extraction strategies."""

from typing import Any, Protocol

from document_etl.models import SourceDocument


class ArtifactStrategy(Protocol):
    """Protocol for strategies that extract one artifact collection from a document."""

    def extract(self, document: Any, source: SourceDocument) -> Any:
        """Return extracted artifacts for the given document and source."""
