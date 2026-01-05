#!/usr/bin/env python3
"""Run normalization on a JSONL file and output to file.

Usage:
    python tests/test_normalize.py <input_file> [--output <output_file>] [--config <config_name>]

Examples:
    python tests/test_normalize.py tests/data/raw_articles_2026_01_05_09_20.jsonl
    python tests/test_normalize.py input.jsonl --output tests/output/custom.json
    python tests/test_normalize.py input.jsonl --config service_test
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add news-normalize to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "news-normalize"))

from news_normalize.normalize import run
from news_normalize.utils.config_loader import load_config
from news_normalize.io.read_jsonl import read_jsonl
from news_normalize.io.write_output import write_output


def main():
    parser = argparse.ArgumentParser(
        description="Normalize a JSONL file and output results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to input JSONL file (can be .jsonl or .jsonl.gz)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path (default: tests/output/<input_stem>_normalized.<format>)",
    )
    parser.add_argument(
        "--config", "-c",
        type=str,
        default=None,
        help="Config name (e.g., 'service_test') or path to YAML config file",
    )
    args = parser.parse_args()

    # Load config if specified
    config = None
    if args.config:
        try:
            config = load_config(args.config)
            print(f"Config: {args.config}")
            print(f"  SpaCy model: {config.spacy_model}")
            print(f"  Embeddings enabled: {config.embedding_enabled}")
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    # Validate input file
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    # Determine output format (config > default json)
    output_format = config.output_format if config else "json"

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)
        # Remove .gz if present, then .jsonl
        stem = input_path.name
        if stem.endswith(".gz"):
            stem = stem[:-3]
        if stem.endswith(".jsonl"):
            stem = stem[:-6]
        output_path = output_dir / f"{stem}_normalized.{output_format}"

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print()

    # Read input
    print("Reading input...")
    raw_articles = list(read_jsonl(str(input_path)))
    print(f"Read {len(raw_articles)} articles")

    # Run normalization
    start_time = datetime.now()
    print("Running normalization...")

    try:
        normalized = run(raw_articles, config)
    except Exception as e:
        print(f"Error during normalization: {e}", file=sys.stderr)
        sys.exit(1)

    # Write output
    write_output(normalized, str(output_path))

    elapsed = datetime.now() - start_time
    print(f"Completed in {elapsed.total_seconds():.1f}s")
    print(f"Output written to: {output_path}")


if __name__ == "__main__":
    main()
