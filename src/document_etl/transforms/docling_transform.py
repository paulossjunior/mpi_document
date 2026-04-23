from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional

from document_etl.models import (
    DocumentArtifacts,
    ImageArtifact,
    SourceDocument,
    TableArtifact,
    TextBlock,
)


class DoclingTransform:
    def __init__(
        self,
        image_resolution_scale: float = 2.0,
        document_timeout: float = 120,
    ) -> None:
        self.image_resolution_scale = image_resolution_scale
        self.document_timeout = document_timeout
        self._converter: Optional[Any] = None

    def transform(self, source: SourceDocument) -> DocumentArtifacts:
        try:
            result = self.converter.convert(source.path)
            document = result.document
            return DocumentArtifacts(
                source=source,
                status=str(getattr(result, "status", "success")),
                markdown=document.export_to_markdown(),
                text=self._export_text(document),
                text_blocks=self._extract_text_blocks(document),
                tables=self._extract_tables(document),
                images=self._extract_images(document, source),
                docling_json=document.export_to_dict(),
            )
        except Exception as exc:
            return DocumentArtifacts(
                source=source,
                status="failure",
                errors=[
                    {
                        "stage": "transform",
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "source": asdict(source),
                    }
                ],
            )

    @property
    def converter(self) -> Any:
        if self._converter is None:
            self._converter = self._build_converter()
        return self._converter

    def _build_converter(self) -> Any:
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.document_converter import DocumentConverter, PdfFormatOption

        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True
        pipeline_options.do_table_structure = True
        pipeline_options.generate_page_images = True
        pipeline_options.generate_picture_images = True
        pipeline_options.images_scale = self.image_resolution_scale
        pipeline_options.document_timeout = self.document_timeout

        return DocumentConverter(
            allowed_formats=[InputFormat.PDF, InputFormat.IMAGE],
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
            },
        )

    @staticmethod
    def _export_text(document: Any) -> str:
        if hasattr(document, "export_to_text"):
            return document.export_to_text()
        return document.export_to_markdown()

    def _extract_text_blocks(self, document: Any) -> List[TextBlock]:
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
                    label=self._stringify(getattr(element, "label", None)),
                    text=text,
                    page_no=self._first_page_no(element),
                    self_ref=getattr(element, "self_ref", None),
                    provenance=self._provenance(element),
                )
            )
        return blocks

    def _extract_tables(self, document: Any) -> List[TableArtifact]:
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
                    markdown=self._safe_call(table, "export_to_markdown", document),
                    html=self._safe_call(table, "export_to_html", document),
                    dataframe=dataframe,
                    page_no=self._first_page_no(table),
                    self_ref=getattr(table, "self_ref", None),
                    provenance=self._provenance(table),
                )
            )
        return tables

    def _extract_images(self, document: Any, source: SourceDocument) -> List[ImageArtifact]:
        from docling_core.types.doc import PictureItem

        images: List[ImageArtifact] = []

        for page_no, page in getattr(document, "pages", {}).items():
            page_image = getattr(getattr(page, "image", None), "pil_image", None)
            if page_image is None:
                continue
            images.append(
                ImageArtifact(
                    index=len(images) + 1,
                    kind="page",
                    filename=f"page_{int(page_no):03d}.png",
                    image=page_image,
                    page_no=int(page_no),
                )
            )

        if hasattr(document, "iterate_items"):
            picture_counter = 0
            for element, _level in document.iterate_items():
                if not isinstance(element, PictureItem):
                    continue
                picture = element.get_image(document)
                if picture is None:
                    continue
                picture_counter += 1
                images.append(
                    ImageArtifact(
                        index=len(images) + 1,
                        kind="picture",
                        filename=f"picture_{picture_counter:03d}.png",
                        image=picture,
                        page_no=self._first_page_no(element),
                        self_ref=getattr(element, "self_ref", None),
                        provenance=self._provenance(element),
                    )
                )

        if not images and source.extension in {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}:
            source_image = self._load_source_image(source)
            if source_image is not None:
                images.append(
                    ImageArtifact(
                        index=1,
                        kind="source",
                        filename="source_image.png",
                        image=source_image,
                    )
                )

        return images

    @staticmethod
    def _load_source_image(source: SourceDocument) -> Any:
        try:
            from PIL import Image

            return Image.open(source.path).convert("RGB")
        except Exception:
            return None

    @staticmethod
    def _safe_call(element: Any, method_name: str, document: Any) -> str:
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
    def _first_page_no(element: Any) -> Optional[int]:
        provenance = getattr(element, "prov", None) or []
        if not provenance:
            return None
        return getattr(provenance[0], "page_no", None)

    @staticmethod
    def _provenance(element: Any) -> List[Dict[str, Any]]:
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
    def _stringify(value: Any) -> Optional[str]:
        if value is None:
            return None
        return getattr(value, "value", str(value))
