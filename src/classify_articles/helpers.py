"""Helper functions for classify_articles CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common.cli_helpers import parse_date

DEFAULT_MODEL = "ContextNews/news-classifier"


def log_classification_results(results: list, articles: list[dict], logger) -> None:  # type: ignore[type-arg]
    titles_by_id = {a["id"]: a.get("title", "untitled") for a in articles}
    for result in results:
        logger.info(
            "  %s | %s | topics=%s",
            result.article_id,
            titles_by_id.get(result.article_id, "untitled"),
            result.topics,
        )


def parse_classify_articles_args() -> argparse.Namespace:
    """Parse CLI arguments for classify_articles."""

    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Classify articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-classify articles that already have topic assignments",
    )

    # Model options
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"HuggingFace text-classification model (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size for inference (default: 32)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Minimum sigmoid score for a label to be included (default: 0.5)",
    )
    parser.add_argument(
        "--word-limit",
        type=int,
        default=None,
        help="Max words in input text (default: no limit)",
    )

    # Output options
    parser.add_argument("--load-object-store", action="store_true", help="Upload results to object store")
    parser.add_argument("--load-db", action="store_true", help="Load topics into DB")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    return parser.parse_args()
