"""CLI entry point for news normalization."""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from news_normalize.utils.config_loader import load_config, build_output_key
from news_normalize.io.read_jsonl import read_jsonl
from news_normalize.io.s3 import list_jsonl_files
from news_normalize.io.write_output import write_output
from news_normalize.normalize import run

logger = logging.getLogger(__name__)


def discover_input_files(config) -> list[str]:
    """Discover input files based on config storage mode."""
    if config.storage == "local":
        input_dir = Path(config.input_dir)
        files = list(input_dir.glob("*.jsonl")) + list(input_dir.glob("*.jsonl.gz"))
        return [str(f) for f in files]
    else:
        return list_jsonl_files(config.bucket, config.input_prefix)


def read_all_articles(input_paths: list[str]) -> list[dict]:
    """Read all articles from input files."""
    articles = []
    for path in input_paths:
        logger.info(f"Reading {path}")
        articles.extend(read_jsonl(path))
    return articles


def build_output_path(config, run_timestamp: str) -> str:
    """Build output path based on config."""
    if config.storage == "local" or config.output_local:
        output_dir = Path(config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        return str(output_dir / f"normalized_{run_timestamp}.{config.output_format}")
    else:
        output_key = build_output_key(config, run_timestamp)
        return f"s3://{config.bucket}/{output_key}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize news articles")
    parser.add_argument(
        "--config",
        default="prod",
        help="Config name (test/prod) or path to YAML file (default: prod)",
    )
    parser.add_argument(
        "--period",
        help="Override period for S3 mode (YYYY-MM-DD), defaults to today",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    config = load_config(args.config)

    # Override period if provided via CLI
    if args.period:
        config.period = args.period

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Discover and read input files
    input_files = discover_input_files(config)
    if not input_files:
        logger.warning("No input files found")
        return

    logger.info(f"Found {len(input_files)} input files")
    raw_articles = read_all_articles(input_files)

    if not raw_articles:
        logger.warning("No articles found in input files")
        return

    # Run normalization
    normalized = run(raw_articles, config)

    # Write output
    output_path = build_output_path(config, run_timestamp)
    write_output(normalized, output_path)
    logger.info(f"Wrote {len(normalized)} articles to {output_path}")


if __name__ == "__main__":
    main()
