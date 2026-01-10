"""Integration tests using test config (local storage, no S3/Postgres)."""

from pathlib import Path

import pytest

from news_ingest.cli import load_config
from news_ingest.config import Config
from news_ingest.ingest import generate_article_id, normalize_article


@pytest.fixture
def config() -> Config:
    """Load test configuration with resolution disabled."""
    cfg = load_config("test")
    cfg.resolve_enabled = False  # Disable for unit tests
    return cfg


@pytest.fixture
def output_dir():
    """Get the test output directory."""
    path = Path(__file__).parent / "output"
    path.mkdir(exist_ok=True)
    return path


def test_article_id_is_deterministic():
    """Verify article_id generation is stable."""
    id1 = generate_article_id("bbc", "https://example.com/article")
    id2 = generate_article_id("bbc", "https://example.com/article")
    id3 = generate_article_id("reuters", "https://example.com/article")

    assert id1 == id2  # Same source + URL = same ID
    assert id1 != id3  # Different source = different ID


def test_config_loads_correctly(config):
    """Verify test config loads with expected values."""
    assert config.storage_backend == "local"
    assert config.state_backend == "memory"
    assert config.output_format == "jsonl"
    assert config.output_compress is False
    assert config.resolve_enabled is False  # Disabled by fixture
    assert "bbc" in config.sources


def test_ingest_with_test_config(config, output_dir):
    """Run ingestion with test config, verify JSONL output."""
    import json
    from datetime import datetime, timezone, timedelta
    from news_ingest.sources import bbc
    from news_ingest.storage.s3 import upload_articles

    # Fetch a few articles
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    fetched_at = datetime.now(timezone.utc)

    articles = []
    for raw in bbc.fetch_articles(since):
        normalized = normalize_article(raw, "bbc", fetched_at, config)
        articles.append(normalized)
        if len(articles) >= 5:  # Limit for faster tests
            break

    if not articles:
        pytest.skip("No articles fetched from BBC")

    # Upload (goes to local JSONL due to test config)
    output_path = upload_articles(articles, fetched_at, config)

    # Verify output
    assert Path(output_path).exists()
    assert output_path.endswith(".jsonl")

    # Verify JSONL content
    with open(output_path) as f:
        rows = [json.loads(line) for line in f]

    assert len(rows) == len(articles)
    assert rows[0]["source"] == "bbc"
    assert "headline" in rows[0]
    assert "url" in rows[0]
    # Verify new schema fields
    assert "content" in rows[0]
    assert "resolution" in rows[0]
    assert "success" in rows[0]["resolution"]
    assert "method" in rows[0]["resolution"]

    print(f"\nWritten {len(rows)} articles to {output_path}")
