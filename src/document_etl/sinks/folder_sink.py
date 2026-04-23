from __future__ import annotations

import json
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

from document_etl.models import DocumentArtifacts


class FolderSink:
    def __init__(self, sink_dir: Path) -> None:
        self.sink_dir = sink_dir

    def write(self, artifacts: DocumentArtifacts) -> Path:
        document_id = artifacts.source.document_id
        # Contract: every sink write owns exactly one document folder.
        document_dir = self.sink_dir / document_id
        if document_dir.exists():
            shutil.rmtree(document_dir)

        text_dir = document_dir / "text"
        tables_dir = document_dir / "tables"
        images_dir = document_dir / "images"
        source_dir = document_dir / "source"
        docling_dir = document_dir / "docling"
        errors_dir = document_dir / "errors"

        for directory in (
            document_dir,
            text_dir,
            tables_dir,
            images_dir,
            source_dir,
            docling_dir,
            errors_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

        self._write_json(document_dir / "metadata.json", self._metadata(artifacts))
        self._write_json(docling_dir / "document.json", artifacts.docling_json)
        self._write_text(text_dir / "content.md", artifacts.markdown)
        self._write_text(text_dir / "content.txt", artifacts.text)
        self._write_jsonl(text_dir / "blocks.jsonl", [asdict(block) for block in artifacts.text_blocks])
        self._write_tables(tables_dir, artifacts)
        self._write_images(images_dir, artifacts)
        self._write_source_file(source_dir, artifacts)
        self._write_jsonl(errors_dir / "errors.jsonl", artifacts.errors)
        return document_dir

    def _write_tables(self, tables_dir: Path, artifacts: DocumentArtifacts) -> None:
        index_rows = []
        for table in artifacts.tables:
            table_id = f"table_{table.index:03d}"
            index_rows.append(
                {
                    "index": table.index,
                    "page_no": table.page_no,
                    "self_ref": table.self_ref,
                    "markdown_path": f"{table_id}.md",
                    "html_path": f"{table_id}.html",
                    "csv_path": f"{table_id}.csv" if table.dataframe is not None else None,
                    "provenance": table.provenance,
                }
            )
            self._write_text(tables_dir / f"{table_id}.md", table.markdown)
            self._write_text(tables_dir / f"{table_id}.html", table.html)
            if table.dataframe is not None:
                table.dataframe.to_csv(tables_dir / f"{table_id}.csv", index=False)

        self._write_jsonl(tables_dir / "tables.jsonl", index_rows)

    def _write_images(self, images_dir: Path, artifacts: DocumentArtifacts) -> None:
        index_rows = []
        for image in artifacts.images:
            image_path = images_dir / image.filename
            image.image.save(image_path, format="PNG")
            index_rows.append(
                {
                    "index": image.index,
                    "kind": image.kind,
                    "filename": image.filename,
                    "path": image.filename,
                    "page_no": image.page_no,
                    "self_ref": image.self_ref,
                    "provenance": image.provenance,
                }
            )

        self._write_jsonl(images_dir / "images.jsonl", index_rows)

    @staticmethod
    def _write_source_file(source_dir: Path, artifacts: DocumentArtifacts) -> None:
        target = source_dir / artifacts.source.filename
        shutil.copy2(artifacts.source.path, target)

    @staticmethod
    def _metadata(artifacts: DocumentArtifacts) -> Dict[str, Any]:
        return {
            "document_id": artifacts.source.document_id,
            "source": asdict(artifacts.source),
            "status": artifacts.status,
            "counts": {
                "text_blocks": len(artifacts.text_blocks),
                "tables": len(artifacts.tables),
                "images": len(artifacts.images),
                "errors": len(artifacts.errors),
            },
        }

    @staticmethod
    def _write_text(path: Path, content: str) -> None:
        path.write_text(content or "", encoding="utf-8")

    @staticmethod
    def _write_json(path: Path, content: Any) -> None:
        path.write_text(
            json.dumps(content, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    @staticmethod
    def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
        with path.open("w", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False, default=str))
                file.write("\n")
