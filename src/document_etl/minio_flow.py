from __future__ import annotations

import argparse
import logging
from pathlib import Path

from document_etl.minio_pipeline import SinkToMinioFlow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Upload transformed document sink folders to MinIO.")
    parser.add_argument("--sink", type=Path, default=Path("data/sink"), help="Folder containing document sink folders.")
    parser.add_argument("--endpoint", default="localhost:9000", help="MinIO endpoint.")
    parser.add_argument("--bucket", default="document-etl", help="MinIO bucket name.")
    parser.add_argument("--bucket-per-document", action="store_true", help="Create one MinIO bucket per document folder.")
    parser.add_argument("--secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    flow = SinkToMinioFlow(
        sink_dir=args.sink,
        endpoint=args.endpoint,
        bucket_name=args.bucket,
        bucket_per_document=args.bucket_per_document,
        secure=args.secure,
    )
    uploaded_count = flow.run()
    target = f"MinIO buckets prefixed with {args.bucket}" if args.bucket_per_document else f"MinIO bucket {args.bucket}"
    print(f"Uploaded {uploaded_count} object(s) to {target}.")


if __name__ == "__main__":
    main()
