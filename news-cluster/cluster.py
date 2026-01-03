#!/usr/bin/env python3
"""CLI entry point for news-cluster."""

import argparse
import logging
import sys

from news_cluster.config_loader import load_config
from news_cluster.pipeline import run_pipeline


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main() -> int:
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Cluster news articles into stories")
    parser.add_argument(
        "--config",
        type=str,
        default="prod",
        help="Config name (test/prod) or path to config file",
    )
    parser.add_argument(
        "--period",
        type=str,
        help="Date to process (YYYY-MM-DD), overrides config",
    )
    parser.add_argument(
        "--window",
        type=int,
        help="Number of days to include (overrides config)",
    )

    args = parser.parse_args()

    try:
        config = load_config(args.config)

        # Override period if specified
        if args.period:
            config.period = args.period

        # Override window if specified
        if args.window:
            config.window = args.window

        logger.info("Starting clustering pipeline")
        logger.info("Config: storage=%s, period=%s, window=%d",
                   config.storage, config.period, config.window)

        result = run_pipeline(config)

        logger.info("Pipeline complete")
        logger.info("  Articles loaded: %d", result.get("articles_loaded", 0))
        logger.info("  Stories created: %d", result.get("stories_created", 0))
        logger.info("  Articles clustered: %d", result.get("articles_clustered", 0))
        logger.info("  Articles noise: %d", result.get("articles_noise", 0))

        return 0

    except FileNotFoundError as e:
        logger.error("Config not found: %s", e)
        return 1
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
