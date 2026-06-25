"""Tests for search_latest Lambda handler helper functions."""

from datetime import UTC, datetime, timedelta

import pytest


# -- _parse_geos_cf_datetime --------------------------------------------------


def test_parse_valid_filename(handler) -> None:
    dt = handler._parse_geos_cf_datetime(
        "http://example.com/file.20240415_1200z.nc4"
    )
    assert dt == datetime(2024, 4, 15, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "url",
    ["file.20200101_0000Z.nc4", "file.20200101_0000z.nc4"],
)
def test_parse_case_insensitive_z(handler, url: str) -> None:
    assert handler._parse_geos_cf_datetime(url) == datetime(
        2020, 1, 1, 0, 0, tzinfo=UTC
    )


@pytest.mark.parametrize(
    "url, expected_hour, expected_minute",
    [
        ("file.20200630_2359z.nc4", 23, 59),
        ("file.20200630_0130z.nc4", 1, 30),
        ("file.20200630_1145z.nc4", 11, 45),
    ],
)
def test_parse_time_components(
    handler, url: str, expected_hour: int, expected_minute: int
) -> None:
    dt = handler._parse_geos_cf_datetime(url)
    assert dt is not None
    assert dt.hour == expected_hour
    assert dt.minute == expected_minute


def test_parse_no_match_returns_none(handler) -> None:
    assert handler._parse_geos_cf_datetime("no_datetime_here.nc4") is None


@pytest.mark.parametrize(
    "url",
    [
        "file.20200631_1200z.nc4",
        "file.20201301_1200z.nc4",
        "file.20200101_2500z.nc4",
        "file.20200101_1260z.nc4",
    ],
)
def test_parse_invalid_date_returns_none(handler, url: str) -> None:
    assert handler._parse_geos_cf_datetime(url) is None


# -- _resolve_queue_url --------------------------------------------------------


def test_resolve_queue_url_process_file_env(
    handler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROCESS_FILE_QUEUE_URL", "https://sqs.us-east-1/123/process")
    monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
    assert handler._resolve_queue_url() == "https://sqs.us-east-1/123/process"


def test_resolve_queue_url_sqs_fallback(
    handler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PROCESS_FILE_QUEUE_URL", raising=False)
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.us-east-1/123/fallback")
    assert handler._resolve_queue_url() == "https://sqs.us-east-1/123/fallback"


def test_resolve_queue_url_none_when_unset(
    handler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PROCESS_FILE_QUEUE_URL", raising=False)
    monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
    assert handler._resolve_queue_url() is None
