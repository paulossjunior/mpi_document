from __future__ import annotations

"""Strategy for extracting table artifacts from a Docling document."""

from typing import Any, List

from document_etl.models import SourceDocument, TableArtifact
from document_etl.transforms.extractors.document_value_adapter import DocumentValueAdapter


class TableStrategy:
    """Extract tables in markdown, HTML and dataframe-friendly form."""

    def extract(self, document: Any, source: SourceDocument) -> List[TableArtifact]:
        tables: List[TableArtifact] = []
        for index, table in enumerate(getattr(document, "tables", []), start=1):
            dataframe = None
            try:
                dataframe = table.export_to_dataframe(doc=document)
            except TypeError:
                dataframe = table.export_to_dataframe()
            except Exception:
                dataframe = None

            tables.append(
                TableArtifact(
                    index=index,
                    markdown=DocumentValueAdapter.safe_call(table, "export_to_markdown", document),
                    html=DocumentValueAdapter.safe_call(table, "export_to_html", document),
                    dataframe=dataframe,
                    page_no=DocumentValueAdapter.first_page_no(table),
                    self_ref=getattr(table, "self_ref", None),
                    provenance=DocumentValueAdapter.provenance(table),
                )
            )
        return tables
