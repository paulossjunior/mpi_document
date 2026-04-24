from __future__ import annotations

"""Adapters for Docling objects whose APIs vary across versions and types."""

from typing import Any, Dict, List, Optional


class DocumentValueAdapter:
    """Normalize common Docling access patterns into stable helper methods."""

    @staticmethod
    def export_text(document: Any) -> str:
        """Export plain text when available, otherwise fall back to markdown."""
        if hasattr(document, "export_to_text"):
            return document.export_to_text()
        return document.export_to_markdown()

    @staticmethod
    def safe_call(element: Any, method_name: str, document: Any) -> str:
        """Call Docling exporters defensively across API shape differences."""
        method = getattr(element, method_name, None)
        if method is None:
            return ""
        try:
            return method(doc=document)
        except TypeError:
            try:
                return method(document)
            except TypeError:
                return method()
        except Exception:
            return ""

    @staticmethod
    def first_page_no(element: Any) -> Optional[int]:
        """Return the first known page number from Docling provenance."""
        provenance = getattr(element, "prov", None) or []
        if not provenance:
            return None
        return getattr(provenance[0], "page_no", None)

    @staticmethod
    def provenance(element: Any) -> List[Dict[str, Any]]:
        """Serialize provenance objects into JSON-friendly dictionaries."""
        provenance = []
        for item in getattr(element, "prov", None) or []:
            if hasattr(item, "model_dump"):
                provenance.append(item.model_dump(mode="json", exclude_none=True))
            elif hasattr(item, "dict"):
                provenance.append(item.dict())
            else:
                provenance.append({"repr": repr(item)})
        return provenance

    @staticmethod
    def stringify(value: Any) -> Optional[str]:
        """Convert enum-like Docling labels into plain strings."""
        if value is None:
            return None
        return getattr(value, "value", str(value))
