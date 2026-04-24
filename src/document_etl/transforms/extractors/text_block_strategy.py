from __future__ import annotations

"""Strategy for extracting text blocks from a Docling document."""

from typing import Any, List

from document_etl.models import SourceDocument, TextBlock
from document_etl.transforms.extractors.document_value_adapter import DocumentValueAdapter


class TextBlockStrategy:
    """Extract linearized text blocks with lightweight provenance metadata."""

    def extract(self, document: Any, source: SourceDocument) -> List[TextBlock]:
        blocks: List[TextBlock] = []
        if not hasattr(document, "iterate_items"):
            return blocks

        for index, (element, _level) in enumerate(document.iterate_items(), start=1):
            text = getattr(element, "text", None)
            if not text:
                continue

            blocks.append(
                TextBlock(
                    index=index,
                    label=DocumentValueAdapter.stringify(getattr(element, "label", None)),
                    text=text,
                    page_no=DocumentValueAdapter.first_page_no(element),
                    self_ref=getattr(element, "self_ref", None),
                    provenance=DocumentValueAdapter.provenance(element),
                )
            )
        return blocks
