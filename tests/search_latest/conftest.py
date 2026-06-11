"""Shared fixtures for search_latest handler tests.

Adds the handler directory to sys.path so ``import handler`` works
despite ``lambda`` being a Python reserved keyword.
"""

import os
import sys

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "search_latest"),
)

import handler as _handler_module  # noqa: E402


@pytest.fixture()
def handler():
    """Return the search_latest handler module."""
    return _handler_module
