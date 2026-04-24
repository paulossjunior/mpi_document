from __future__ import annotations

"""Factory helpers for building configured Docling converters."""

from typing import Any


class DoclingConverterFactory:
    """Build configured Docling ``DocumentConverter`` instances for this project."""

    @staticmethod
    def create(
        image_resolution_scale: float,
        document_timeout: float,
        *,
        do_ocr: bool = True,
        do_table_structure: bool = True,
        generate_page_images: bool = True,
        generate_picture_images: bool = True,
    ) -> Any:
        """Create a converter configured for PDFs and images.

        Args:
            image_resolution_scale: Scale factor used when Docling renders images.
            document_timeout: Maximum time budget passed to Docling for one document.
            do_ocr: Whether OCR should run during PDF processing.
            do_table_structure: Whether table structure extraction should run.
            generate_page_images: Whether Docling should export rendered page images.
            generate_picture_images: Whether Docling should export figure images.

        Returns:
            A configured Docling ``DocumentConverter`` instance.
        """
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.document_converter import DocumentConverter, PdfFormatOption

        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = do_ocr
        pipeline_options.do_table_structure = do_table_structure
        pipeline_options.generate_page_images = generate_page_images
        pipeline_options.generate_picture_images = generate_picture_images
        pipeline_options.images_scale = image_resolution_scale
        pipeline_options.document_timeout = document_timeout

        return DocumentConverter(
            allowed_formats=[InputFormat.PDF, InputFormat.IMAGE],
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
            },
        )
