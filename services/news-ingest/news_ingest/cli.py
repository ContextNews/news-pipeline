"""CLI entry point for news ingestion."""

import argparse
import logging
import sys
from pathlib import Path

import yaml

from news_ingest.config import Config, parse_config
from news_ingest.ingest import ingest
from news_ingest.schema import IngestConfig

def load_config(config_path: str, config_type: Any) -> Config:
    """Load and parse config from yaml file."""
    try:
        path = Path(config_path)
        with open(path) as f:
            data = yaml.safe_load(f)

        return IngestConfig(**data)
    except Exception as e:
        logging.error(f"Failed to load config from {config_path}: {e}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest news articles from configured sources"
    )
    parser.add_argument(
        "--config",
        default="prod",
        help="Config name (test/prod) or path to YAML file. Defaults to 'prod'",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_config(args.config)
    exit_code = ingest(config)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
