"""Common CLI helper utilities."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


def setup_logging() -> None:
    """Configure standard logging format for CLI tools."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def parse_date(value: str, field_name: str = "date") -> date:
    """Parse a date string for argparse arguments.

    Args:
        value: Date string in YYYY-MM-DD format.
        field_name: Name of the field for error messages.

    Returns:
        Parsed date object.

    Raises:
        argparse.ArgumentTypeError: If the value is not a valid date.
    """
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{field_name} must be YYYY-MM-DD") from exc


def date_to_range(d: date) -> tuple[datetime, datetime]:
    """Convert a date to a datetime range covering the full day.

    Args:
        d: The date to convert.

    Returns:
        Tuple of (start, end) datetimes where start is midnight and end is midnight the next day.
    """
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end


def save_jsonl_local(
    records: list[dict[str, Any]],
    prefix: str,
    timestamp: datetime,
    output_dir: str = "output",
) -> Path:
    """Save records to a local JSONL file.

    Args:
        records: List of dictionaries to save.
        prefix: Filename prefix (e.g., "clustered_articles").
        timestamp: Timestamp to include in filename.
        output_dir: Directory to save to (default: "output").

    Returns:
        Path to the created file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    filename = f"{prefix}_{timestamp.strftime('%Y_%m_%d_%H_%M')}.jsonl"
    filepath = output_path / filename
    with filepath.open("w") as f:
        for record in records:
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    return filepath
