"""Output writers for different formats."""

import json
from pathlib import Path

from news_normalize.extract.schema import NormalizedArticle
from news_normalize.io.s3 import is_s3_path, write_s3_bytes
from news_normalize.io.write_parquet import write_parquet


def write_json(articles: list[NormalizedArticle], path: str) -> None:
    """Write articles to a JSON file (local or S3)."""
    rows = [article.to_dict() for article in articles]

    # Convert datetime objects to ISO strings
    for row in rows:
        for key in ["published_at", "fetched_at", "normalized_at"]:
            if key in row and hasattr(row[key], "isoformat"):
                row[key] = row[key].isoformat()

    content = json.dumps(rows, indent=2)

    if is_s3_path(path):
        write_s3_bytes(path, content.encode("utf-8"))
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(content)


def write_output(articles: list[NormalizedArticle], path: str) -> None:
    """Write articles to output file, detecting format from extension."""
    if path.endswith(".json"):
        write_json(articles, path)
    else:
        write_parquet(articles, path)
