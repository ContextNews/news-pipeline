"""Integration test configuration.

Integration tests require a real DATABASE_URL and will be skipped if it is not set.
Run them explicitly with:

    poetry run pytest -m integration -s --log-cli-level=INFO
"""

import logging
import os

import pytest
from dotenv import load_dotenv

load_dotenv()

# Allow NEON_DB_URL_DIRECT or NEON_DB_URL_POOLED as fallbacks for DATABASE_URL
if not os.getenv("DATABASE_URL"):
    fallback = os.getenv("NEON_DB_URL_DIRECT") or os.getenv("NEON_DB_URL_POOLED")
    if fallback:
        os.environ["DATABASE_URL"] = fallback


def pytest_configure(config):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


@pytest.fixture(scope="session", autouse=True)
def require_database():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL is not set — skipping integration tests")
