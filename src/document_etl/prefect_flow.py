from __future__ import annotations

"""Prefect orchestration entrypoint for the MinIO-first ETL flow."""

from contextlib import contextmanager
import logging
import os
from pathlib import Path
from typing import Iterator

from prefect import flow, get_run_logger

from document_etl.minio_etl_flow import DEFAULT_LOG_LEVEL, LOG_DIR, LOG_FILE_PATH
from document_etl.minio_etl_pipeline import MinioDocumentEtlFlow


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def configure_prefect_logging(log_level: str) -> None:
    """Add a file handler without replacing Prefect's own logging handlers."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    resolved_level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    log_path = str(LOG_FILE_PATH.resolve())

    root_logger = logging.getLogger()
    root_logger.setLevel(resolved_level)

    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler) and Path(handler.baseFilename).resolve() == LOG_FILE_PATH.resolve():
            handler.setLevel(resolved_level)
            handler.setFormatter(formatter)
            return

    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
    file_handler.setLevel(resolved_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


class PrefectRunLoggerHandler(logging.Handler):
    """Forward selected standard logging records into the active Prefect flow run."""

    def __init__(self, prefect_logger: logging.Logger) -> None:
        super().__init__()
        self.prefect_logger = prefect_logger

    def emit(self, record: logging.LogRecord) -> None:
        if not record.name.startswith("document_etl"):
            return

        message = self.format(record)
        if record.levelno >= logging.ERROR:
            self.prefect_logger.error(message)
        elif record.levelno >= logging.WARNING:
            self.prefect_logger.warning(message)
        elif record.levelno >= logging.INFO:
            self.prefect_logger.info(message)
        else:
            self.prefect_logger.debug(message)


@contextmanager
def prefect_run_log_bridge(prefect_logger: logging.Logger, log_level: str) -> Iterator[None]:
    """Mirror document_etl logs into the Prefect run logger during one flow run."""
    resolved_level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter("%(name)s: %(message)s")
    handler = PrefectRunLoggerHandler(prefect_logger)
    handler.setLevel(resolved_level)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    try:
        yield
    finally:
        root_logger.removeHandler(handler)


@flow(name="minio-document-etl")
def prefect_minio_document_etl_flow(
    source_bucket: str = os.getenv("MINIO_SOURCE_BUCKET", "source"),
    endpoint: str = os.getenv("MINIO_ENDPOINT", "minio:9000"),
    bucket: str = os.getenv("MINIO_SINK_BUCKET", "sink"),
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure: bool = _env_bool("MINIO_SECURE", False),
    source_prefix: str = os.getenv("MINIO_SOURCE_PREFIX", ""),
    processing_prefix: str = os.getenv("MINIO_PROCESSING_PREFIX", "processing/"),
    failed_prefix: str = os.getenv("MINIO_FAILED_PREFIX", "failed/"),
    recovery_timeout_seconds: float = float(os.getenv("MINIO_PROCESSING_RECOVERY_TIMEOUT_SECONDS", "300")),
    sink_prefix: str = os.getenv("MINIO_SINK_PREFIX", ""),
    log_level: str = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL),
) -> int:
    """Run a single ETL pass under Prefect orchestration."""
    configure_prefect_logging(log_level)
    logger = get_run_logger()
    logger.info(
        "starting Prefect ETL run source_bucket=%s sink_bucket=%s endpoint=%s",
        source_bucket,
        bucket,
        endpoint,
    )

    flow_runner = MinioDocumentEtlFlow(
        source_bucket=source_bucket,
        endpoint=endpoint,
        sink_bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        source_prefix=source_prefix,
        processing_prefix=processing_prefix,
        failed_prefix=failed_prefix,
        recovery_timeout_seconds=recovery_timeout_seconds,
        sink_prefix=sink_prefix,
    )
    with prefect_run_log_bridge(logger, log_level):
        uploaded_count = flow_runner.run()
    logger.info("finished Prefect ETL run uploaded_objects=%s", uploaded_count)
    return uploaded_count


def serve_main() -> None:
    """Create and serve a Prefect deployment that runs the ETL on an interval."""
    log_level = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL)
    configure_prefect_logging(log_level)

    deployment_name = os.getenv("PREFECT_DEPLOYMENT_NAME", "minio-document-etl")
    interval_seconds = float(os.getenv("PREFECT_ETL_INTERVAL_SECONDS", "10"))
    tag_string = os.getenv("PREFECT_DEPLOYMENT_TAGS", "minio,etl,docling")
    tags = [tag.strip() for tag in tag_string.split(",") if tag.strip()]

    prefect_minio_document_etl_flow.serve(
        name=deployment_name,
        interval=interval_seconds,
        pause_on_shutdown=False,
        limit=1,
        tags=tags or None,
    )
