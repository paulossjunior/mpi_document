from __future__ import annotations

import argparse
import logging
from pathlib import Path

from document_etl.pipeline import DocumentEtlFlow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Source -> Docling Transform -> Folder Sink.")
    parser.add_argument("--source", type=Path, default=Path("data/source"), help="Folder containing PDF/image inputs.")
    parser.add_argument("--sink", type=Path, default=Path("data/sink"), help="Folder where extracted artifacts are written.")
    parser.add_argument("--recursive", action="store_true", help="Read source folder recursively.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    flow = DocumentEtlFlow(
        source_dir=args.source,
        sink_dir=args.sink,
        recursive=args.recursive,
    )
    output_paths = flow.run()
    print(f"Processed {len(output_paths)} document(s).")
    for path in output_paths:
        print(path)


if __name__ == "__main__":
    main()

