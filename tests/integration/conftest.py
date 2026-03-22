"""Integration test configuration.

Integration tests require a real DATABASE_URL and will be skipped if it is not set.
Run them explicitly with:

    poetry run pytest -m integration -s --log-cli-level=INFO
"""

import logging
import os

import pytest


def pytest_configure(config):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


@pytest.fixture(scope="session", autouse=True)
def require_database():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL is not set — skipping integration tests")
