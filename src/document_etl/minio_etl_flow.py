from __future__ import annotations

import argparse
import logging

from document_etl.minio_etl_pipeline import MinioDocumentEtlFlow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Source bucket -> Docling Transform -> MinIO bucket(s).")
    parser.add_argument("--source-bucket", required=True, help="MinIO bucket containing PDF/image inputs.")
    parser.add_argument("--endpoint", default="localhost:9000", help="MinIO endpoint.")
    parser.add_argument("--bucket", default="document-etl", help="Destination bucket prefix.")
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key.")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key.")
    parser.add_argument("--secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument("--source-prefix", default="source/", help="Prefix for new source objects.")
    parser.add_argument("--processing-prefix", default="processing/", help="Prefix for claimed objects in processing.")
    parser.add_argument("--failed-prefix", default="failed/", help="Prefix for failed objects.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    flow = MinioDocumentEtlFlow(
        source_bucket=args.source_bucket,
        endpoint=args.endpoint,
        bucket_name=args.bucket,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=args.secure,
        source_prefix=args.source_prefix,
        processing_prefix=args.processing_prefix,
        failed_prefix=args.failed_prefix,
    )
    uploaded_count = flow.run()
    print(f"Uploaded {uploaded_count} object(s) after transform from source bucket {args.source_bucket}.")


if __name__ == "__main__":
    main()
