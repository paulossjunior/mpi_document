from __future__ import annotations

import argparse
import logging
import time

from document_etl.minio_etl_pipeline import MinioDocumentEtlFlow

log = logging.getLogger(__name__)


class MinioDocumentWorker:
    def __init__(
        self,
        source_bucket: str,
        endpoint: str | None = None,
        bucket_name: str = "document-etl",
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = False,
        poll_interval: float = 10.0,
        source_prefix: str = "source/",
        processing_prefix: str = "processing/",
        failed_prefix: str = "failed/",
    ) -> None:
        self.flow = MinioDocumentEtlFlow(
            source_bucket=source_bucket,
            endpoint=endpoint,
            bucket_name=bucket_name,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            source_prefix=source_prefix,
            processing_prefix=processing_prefix,
            failed_prefix=failed_prefix,
        )
        self.poll_interval = poll_interval

    def run_forever(self) -> None:
        while True:
            try:
                uploaded_count = self.flow.run()
                if uploaded_count:
                    log.info("worker cycle finished uploaded_count=%s", uploaded_count)
                else:
                    log.debug("worker cycle finished with no new documents")
            except Exception:
                log.exception("worker cycle failed")

            time.sleep(self.poll_interval)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a worker for Source bucket -> Docling Transform -> MinIO bucket(s).")
    parser.add_argument("--source-bucket", required=True, help="MinIO bucket containing PDF/image inputs.")
    parser.add_argument("--endpoint", default="localhost:9000", help="MinIO endpoint.")
    parser.add_argument("--bucket", default="document-etl", help="Destination bucket prefix.")
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key.")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key.")
    parser.add_argument("--secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument("--poll-interval", type=float, default=10.0, help="Polling interval in seconds.")
    parser.add_argument("--source-prefix", default="source/", help="Prefix for new source objects.")
    parser.add_argument("--processing-prefix", default="processing/", help="Prefix for claimed objects in processing.")
    parser.add_argument("--failed-prefix", default="failed/", help="Prefix for failed objects.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    worker = MinioDocumentWorker(
        source_bucket=args.source_bucket,
        endpoint=args.endpoint,
        bucket_name=args.bucket,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=args.secure,
        poll_interval=args.poll_interval,
        source_prefix=args.source_prefix,
        processing_prefix=args.processing_prefix,
        failed_prefix=args.failed_prefix,
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
