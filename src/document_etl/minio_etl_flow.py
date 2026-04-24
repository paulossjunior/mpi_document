from __future__ import annotations

"""CLI entrypoint for one-shot and continuous MinIO ETL execution."""

import argparse
import logging
import os
import time
from pathlib import Path

from document_etl.minio_etl_pipeline import MinioDocumentEtlFlow

WORKER_POLL_INTERVAL_SECONDS = 10.0
DEFAULT_LOG_LEVEL = "INFO"
LOG_DIR = Path(os.getenv("LOG_DIR", "log"))
LOG_FILE_PATH = LOG_DIR / "minio_etl.log"
log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the MinIO-first ETL entrypoint."""
    parser = argparse.ArgumentParser(description="Run Source bucket -> Docling Transform -> sink bucket.")
    parser.add_argument(
        "--source-bucket",
        default=os.getenv("MINIO_SOURCE_BUCKET", "source"),
        help="MinIO bucket containing PDF/image inputs.",
    )
    parser.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "localhost:9000"), help="MinIO endpoint.")
    parser.add_argument("--bucket", default=os.getenv("MINIO_SINK_BUCKET", "sink"), help="Destination sink bucket.")
    parser.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"), help="MinIO access key.")
    parser.add_argument(
        "--secret-key",
        default=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        help="MinIO secret key.",
    )
    parser.add_argument("--secure", action="store_true", help="Use HTTPS for MinIO.")
    parser.add_argument(
        "--source-prefix",
        default=os.getenv("MINIO_SOURCE_PREFIX", ""),
        help="Optional prefix for new source objects. Default: process the whole bucket root recursively.",
    )
    parser.add_argument(
        "--processing-prefix",
        default=os.getenv("MINIO_PROCESSING_PREFIX", "processing/"),
        help="Prefix for claimed objects in processing.",
    )
    parser.add_argument(
        "--failed-prefix",
        default=os.getenv("MINIO_FAILED_PREFIX", "failed/"),
        help="Prefix for failed objects.",
    )
    parser.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=float(os.getenv("MINIO_PROCESSING_RECOVERY_TIMEOUT_SECONDS", "300")),
        help="Age threshold used to recover orphaned objects from processing/.",
    )
    parser.add_argument(
        "--sink-prefix",
        default=os.getenv("MINIO_SINK_PREFIX", ""),
        help="Optional prefix inside the sink bucket.",
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help="Run continuously, polling for new documents every 10 seconds.",
    )
    parser.add_argument("--log-level", default=DEFAULT_LOG_LEVEL, help="Python logging level.")
    return parser


def configure_logging(log_level: str) -> None:
    """Configure console and file logging under the local log directory."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    resolved_level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
    file_handler.setLevel(resolved_level)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(resolved_level)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(resolved_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


def main() -> None:
    """Parse CLI arguments and execute either one-shot or worker mode."""
    args = build_parser().parse_args()
    configure_logging(args.log_level)
    log.info("logging configured level=%s file=%s", args.log_level.upper(), LOG_FILE_PATH)

    flow = MinioDocumentEtlFlow(
        source_bucket=args.source_bucket,
        endpoint=args.endpoint,
        sink_bucket=args.bucket,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=args.secure,
        source_prefix=args.source_prefix,
        processing_prefix=args.processing_prefix,
        failed_prefix=args.failed_prefix,
        recovery_timeout_seconds=args.recovery_timeout_seconds,
        sink_prefix=args.sink_prefix,
    )
    if args.worker:
        log.info(
            "starting worker mode source_bucket=%s sink_bucket=%s interval_seconds=%s",
            args.source_bucket,
            args.bucket,
            WORKER_POLL_INTERVAL_SECONDS,
        )
        while True:
            try:
                uploaded_count = flow.run()
                if uploaded_count:
                    log.info("worker cycle finished uploaded_count=%s", uploaded_count)
                else:
                    log.debug("worker cycle finished with no new documents")
            except Exception:
                log.exception("worker cycle failed")

            log.debug("sleeping before next worker cycle seconds=%s", WORKER_POLL_INTERVAL_SECONDS)
            time.sleep(WORKER_POLL_INTERVAL_SECONDS)
    else:
        log.info("starting one-shot ETL source_bucket=%s sink_bucket=%s", args.source_bucket, args.bucket)
        uploaded_count = flow.run()
        print(f"Uploaded {uploaded_count} object(s) from source bucket {args.source_bucket} into sink bucket {args.bucket}.")


if __name__ == "__main__":
    main()
