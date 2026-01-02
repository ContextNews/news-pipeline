#!/usr/bin/env python3
"""CLI entry point for news normalization."""

import argparse
import logging

from news_normalize.config_loader import load_config
from news_normalize.normalize import run_from_config


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

    run_from_config(config)


if __name__ == "__main__":
    main()
