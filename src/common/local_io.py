"""Local file I/O utilities."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.serialization import serialize_dataclass

logger = logging.getLogger(__name__)


def save_jsonl_records_local(
    records: list[Any],
    prefix: str,
    output_dir: str = "output",
) -> None:
    """
    Save a list of dataclass records to a local JSONL file.

    Handles serialization, builds the filename, and logs the result.

    Args:
        records: List of dataclass objects to save
        prefix: Filename prefix (e.g., "ingested_articles", "embedded_articles")
        output_dir: Directory to save to (default: "output")
    """
    now = datetime.now(timezone.utc)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    filename = f"{prefix}_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
    filepath = output_path / filename

    with filepath.open("w") as f:
        for record in records:
            serialized = serialize_dataclass(record)
            f.write(json.dumps(serialized, default=str, ensure_ascii=False) + "\n")

    logger.info("Saved %d records to %s", len(records), filepath)
