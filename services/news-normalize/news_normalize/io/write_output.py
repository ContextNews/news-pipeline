"""Output writers for different formats."""

import csv
import json
from io import StringIO
from pathlib import Path

from news_normalize.schema import NormalizedArticle
from news_normalize.io.s3 import is_s3_path, write_s3_bytes
from news_normalize.io.write_parquet import write_parquet


# CSV columns - excludes embeddings (too large) and complex nested fields
CSV_COLUMNS = [
    "article_id",
    "source",
    "url",
    "published_at",
    "fetched_at",
    "headline",
    "body",
    "content_clean",
    "ner_model",
    "normalized_at",
    "entity_count",
    "location_count",
    "top_entities",
    "top_locations",
]


def write_csv(articles: list[NormalizedArticle], path: str) -> None:
    """Write articles to a CSV file (local or S3).

    Flattens nested structures for CSV compatibility:
    - entities -> entity_count + top_entities (semicolon-separated)
    - locations -> location_count + top_locations (semicolon-separated)
    - Excludes embedding vectors (too large for CSV)
    """
    rows = []
    for article in articles:
        # Format top entities as "text:type:count" joined by semicolons
        top_entities = ";".join(
            f"{e.text}:{e.type}:{e.count}" for e in article.entities[:10]
        )
        # Format top locations as "name:country_code:confidence" joined by semicolons
        top_locations = ";".join(
            f"{loc.name}:{loc.country_code or 'unknown'}:{loc.confidence:.2f}"
            for loc in article.locations[:5]
        )

        rows.append({
            "article_id": article.article_id,
            "source": article.source,
            "url": article.url,
            "published_at": article.published_at.isoformat() if article.published_at else "",
            "fetched_at": article.fetched_at.isoformat() if article.fetched_at else "",
            "headline": article.headline,
            "body": article.body,
            "content_clean": article.content_clean or "",
            "ner_model": article.ner_model,
            "normalized_at": article.normalized_at.isoformat() if article.normalized_at else "",
            "entity_count": len(article.entities),
            "location_count": len(article.locations),
            "top_entities": top_entities,
            "top_locations": top_locations,
        })

    if is_s3_path(path):
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        write_s3_bytes(path, buffer.getvalue().encode("utf-8"))
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)


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
    elif path.endswith(".csv"):
        write_csv(articles, path)
    else:
        write_parquet(articles, path)
