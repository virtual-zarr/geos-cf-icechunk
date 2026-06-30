import json
import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.typing import LambdaContext
from query_thredds import load_catalog, recurse_catalog
from virtualizarr_processor.processor import Processor


logger = Logger()
tracer = Tracer()

GEOS_CF_FILENAME_PATTERN = re.compile(r"\.(\d{8}_\d{4})z(?:\.\w+)*\.nc4$", re.IGNORECASE)


def _parse_geos_cf_datetime(url: str) -> datetime | None:
    match = GEOS_CF_FILENAME_PATTERN.search(url)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d_%H%M").replace(tzinfo=UTC)
    except ValueError:
        return None


def _resolve_queue_url() -> str | None:
    return os.getenv("PROCESS_FILE_QUEUE_URL") or os.getenv("SQS_QUEUE_URL")


def _discover_last_day_urls(catalog_url: str, file_pattern: str) -> list[dict[str, Any]]:
    """Check yesterday's catalog for available files, falling back to today.

    Returns candidates sorted oldest-first (chronological order).
    """
    base_url = catalog_url.rsplit("/catalog.xml", 1)[0]
    now = datetime.now(UTC)
    # Try yesterday first (most likely to be complete), then today
    days = [now.date() - timedelta(days=1), now.date()]

    discovered: list[dict[str, Any]] = []
    for day in days:
        day_prefix = f"Y{day:%Y}/M{day:%m}/D{day:%d}"
        day_catalog_url = f"{base_url}/{day_prefix}/catalog.xml"
        file_glob = f"**/{file_pattern}"

        try:
            day_catalog = load_catalog(day_catalog_url)
        except Exception as exc:
            logger.warning(
                "Failed to load day catalog, skipping",
                extra={"day_catalog_url": day_catalog_url, "error": str(exc)},
            )
            continue

        day_urls = list(recurse_catalog(catalog=day_catalog, pattern=file_glob))
        logger.info(
            "Day catalog query result",
            extra={
                "day_prefix": day_prefix,
                "matched_url_count": len(day_urls),
                "file_glob": file_glob,
            },
        )

        for url in day_urls:
            dt_utc = _parse_geos_cf_datetime(url)
            if dt_utc:
                discovered.append({"url": url, "dt_utc": dt_utc})

        # If we found files for this day, use them and stop
        if discovered:
            break

    # Sort oldest first so processing happens in chronological order
    return sorted(discovered, key=lambda row: row["dt_utc"])


def _get_stored_times() -> set[datetime]:
    """Return all times already committed to icechunk."""
    processor = Processor(prefix=os.getenv("ICECHUNK_PREFIX", ""))
    _, stored = processor.search_state()
    return stored


def _enqueue_urls(queue_url: str, urls: list[str]) -> None:
    client = boto3.client("sqs")
    client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"urls": urls}),
    )


@tracer.capture_method
def search_latest_data(message: dict[str, Any]) -> None:
    """Check the last day's catalog, find unprocessed files, and queue them in order."""
    catalog_url = os.getenv("THREDDS_ANALYSIS_V2_CATALOG")
    query_pattern = os.getenv("THREDDS_QUERY_PATTERN")
    queue_url = _resolve_queue_url()

    if not catalog_url:
        raise ValueError("THREDDS_ANALYSIS_V2_CATALOG environment variable is required")
    if not query_pattern:
        raise ValueError("THREDDS_QUERY_PATTERN environment variable is required")

    candidates = _discover_last_day_urls(catalog_url, query_pattern)
    if not candidates:
        logger.info("No candidates found in last day catalog")
        return None

    stored_times = _get_stored_times()
    # Filter to only files not yet processed, maintaining chronological order
    eligible = [row for row in candidates if row["dt_utc"] not in stored_times]

    logger.info(
        "Search result",
        extra={
            "candidate_count": len(candidates),
            "eligible_count": len(eligible),
            "stored_time_count": len(stored_times),
        },
    )

    if not eligible:
        logger.info("No new files to process")
        return None

    if not queue_url:
        raise ValueError("SQS queue URL is required to enqueue new files")

    # Enqueue all unprocessed files as a single batch in chronological order
    urls = [item["url"] for item in eligible]
    _enqueue_urls(queue_url, urls)

    logger.info(
        "Enqueued batch of unprocessed files",
        extra={
            "enqueued_count": len(eligible),
            "first_dt": eligible[0]["dt_utc"].isoformat(),
            "last_dt": eligible[-1]["dt_utc"].isoformat(),
        },
    )
    return None


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> PartialItemFailureResponse:
    """Lambda handler to discover and enqueue unprocessed files from the last day."""
    payload = event if isinstance(event, dict) else {}
    _ = search_latest_data(payload)
    logger.info("search_latest complete")
    return {"batchItemFailures": []}
