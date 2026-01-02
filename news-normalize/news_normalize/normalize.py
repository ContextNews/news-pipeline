"""Main entry point for news normalization pipeline."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from news_normalize.config_loader import NormalizeConfig, build_output_key
from news_normalize.extract.locations import rank_locations
from news_normalize.extract.ner import DEFAULT_SPACY_MODEL, SPACY_MODELS, extract_entities_batch
from news_normalize.extract.schema import Entity, NormalizedArticle
from news_normalize.extract.text import clean_text
from news_normalize.io.read_jsonl import read_jsonl
from news_normalize.io.s3 import list_jsonl_files
from news_normalize.io.write_output import write_output

logger = logging.getLogger(__name__)


@dataclass
class PreparedArticle:
    """Intermediate state holding raw fields + cleaned content before NER."""

    # Raw fields from input (preserved as-is)
    article_id: str
    source: str
    url: str
    published_at: datetime
    fetched_at: datetime
    headline: str
    body: str
    content: Optional[str]
    resolution: dict

    # Prepared for NER
    content_clean: Optional[str]

    # Error tracking (for pipeline failures, not content issues)
    error: Optional[str] = None


def prepare_article(raw: dict) -> PreparedArticle:
    """
    Prepare article for batch NER.

    Preserves all raw fields from input and adds content_clean.
    """
    article_id = raw["article_id"]
    source = raw["source"]
    url = raw["url"]
    headline = raw["headline"]
    published_at = datetime.fromisoformat(raw["published_at"].replace("Z", "+00:00"))
    fetched_at = datetime.fromisoformat(raw["fetched_at"].replace("Z", "+00:00"))
    body = raw.get("body", "")
    content = raw.get("content")
    resolution = raw.get("resolution", {})

    # Clean the content (collapse whitespace)
    content_clean = clean_text(content)

    return PreparedArticle(
        article_id=article_id,
        source=source,
        url=url,
        published_at=published_at,
        fetched_at=fetched_at,
        headline=headline,
        body=body,
        content=content,
        resolution=resolution,
        content_clean=content_clean,
    )


def finalize_article(
    prepared: PreparedArticle, entities: list[Entity], ner_model: str
) -> NormalizedArticle:
    """Finalize article with pre-computed entities."""
    locations = rank_locations(entities, prepared.headline) if prepared.content_clean else []

    return NormalizedArticle(
        # Raw fields (preserved)
        article_id=prepared.article_id,
        source=prepared.source,
        url=prepared.url,
        published_at=prepared.published_at,
        fetched_at=prepared.fetched_at,
        headline=prepared.headline,
        body=prepared.body,
        content=prepared.content,
        resolution=prepared.resolution,
        # Added by normalization
        content_clean=prepared.content_clean,
        ner_model=ner_model,
        entities=entities,
        locations=locations,
        normalized_at=datetime.now(timezone.utc),
    )


def make_failed_article(raw: dict, error: str, ner_model: str) -> NormalizedArticle:
    """Create an article record when pipeline processing fails."""
    # Parse timestamps with fallback
    try:
        published_at = datetime.fromisoformat(raw.get("published_at", "").replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        published_at = datetime.now(timezone.utc)

    try:
        fetched_at = datetime.fromisoformat(raw.get("fetched_at", "").replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        fetched_at = datetime.now(timezone.utc)

    return NormalizedArticle(
        # Raw fields (preserved as much as possible)
        article_id=raw.get("article_id", "unknown"),
        source=raw.get("source", "unknown"),
        url=raw.get("url", ""),
        published_at=published_at,
        fetched_at=fetched_at,
        headline=raw.get("headline", ""),
        body=raw.get("body", ""),
        content=raw.get("content"),
        resolution=raw.get("resolution", {"success": False, "error": error}),
        # Added by normalization (empty due to failure)
        content_clean=None,
        ner_model=ner_model,
        entities=[],
        locations=[],
        normalized_at=datetime.now(timezone.utc),
    )


def run(
    input_path: str, output_path: str, spacy_model: str = DEFAULT_SPACY_MODEL
) -> None:
    """
    Run the normalization pipeline.

    Args:
        input_path: Path to input JSONL file (local or s3://)
        output_path: Path to output Parquet file (local or s3://)
        spacy_model: spaCy model key (sm, lg, or trf)
    """
    logger.info(f"Starting normalization: {input_path} -> {output_path}")

    # Resolve model key to full model name
    ner_model = SPACY_MODELS[spacy_model]

    # First pass: prepare all articles
    prepared: list[PreparedArticle] = []
    raw_articles: list[dict] = []  # Keep raw for error recovery

    for idx, raw in enumerate(read_jsonl(input_path)):
        raw_articles.append(raw)
        try:
            prep = prepare_article(raw)
            prepared.append(prep)
            logger.info(f"Prepared {prep.article_id}")
        except Exception as e:
            logger.error(f"Failed to prepare article {raw.get('article_id', 'unknown')}: {e}")
            # Add placeholder to maintain index alignment
            prepared.append(
                PreparedArticle(
                    article_id=raw.get("article_id", "unknown"),
                    source=raw.get("source", "unknown"),
                    url=raw.get("url", ""),
                    published_at=datetime.now(timezone.utc),
                    fetched_at=datetime.now(timezone.utc),
                    headline=raw.get("headline", ""),
                    body=raw.get("body", ""),
                    content=raw.get("content"),
                    resolution=raw.get("resolution", {}),
                    content_clean=None,
                    error=str(e),
                )
            )

    # Batch NER on all cleaned content
    logger.info(f"Running batch NER on {len(prepared)} articles")
    texts = [p.content_clean for p in prepared]
    all_entities = extract_entities_batch(texts, model_key=spacy_model)

    # Second pass: finalize articles with entities
    articles: list[NormalizedArticle] = []
    for i, prep in enumerate(prepared):
        if prep.error:
            # Pipeline failed for this article
            articles.append(make_failed_article(raw_articles[i], prep.error, ner_model))
        else:
            articles.append(finalize_article(prep, all_entities[i], ner_model))

    write_output(articles, output_path)
    logger.info(f"Wrote {len(articles)} articles to {output_path}")


def run_from_config(config: NormalizeConfig) -> None:
    """
    Run normalization based on config.

    For S3 storage: Discovers all JSONL files for the configured period,
    processes them, and writes output to S3 with timestamped filename.

    For local storage: Processes all JSONL files in input_dir and writes
    output to output_dir.

    Args:
        config: Normalization configuration
    """
    if config.storage == "local":
        _run_local(config)
    else:
        _run_s3(config)


def _run_local(config: NormalizeConfig) -> None:
    """Run normalization with local file storage."""
    from pathlib import Path

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    input_dir = Path(config.input_dir)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting local normalization: {input_dir} -> {output_dir}")

    # Find all JSONL files in input directory
    input_files = list(input_dir.glob("*.jsonl")) + list(input_dir.glob("*.jsonl.gz"))

    if not input_files:
        logger.warning(f"No JSONL files found in {input_dir}")
        return

    logger.info(f"Found {len(input_files)} files")

    # Resolve model key to full model name
    ner_model = SPACY_MODELS[config.spacy_model]

    # First pass: prepare all articles from all files
    prepared: list[PreparedArticle] = []
    raw_articles: list[dict] = []

    for file_path in input_files:
        logger.info(f"Reading {file_path}")
        for raw in read_jsonl(str(file_path)):
            raw_articles.append(raw)
            try:
                prep = prepare_article(raw)
                prepared.append(prep)
                logger.debug(f"Prepared {prep.article_id}")
            except Exception as e:
                logger.error(f"Failed to prepare article {raw.get('article_id', 'unknown')}: {e}")
                prepared.append(
                    PreparedArticle(
                        article_id=raw.get("article_id", "unknown"),
                        source=raw.get("source", "unknown"),
                        url=raw.get("url", ""),
                        published_at=datetime.now(timezone.utc),
                        fetched_at=datetime.now(timezone.utc),
                        headline=raw.get("headline", ""),
                        body=raw.get("body", ""),
                        content=raw.get("content"),
                        resolution=raw.get("resolution", {}),
                        content_clean=None,
                        error=str(e),
                    )
                )

    logger.info(f"Prepared {len(prepared)} articles total")

    # Batch NER on all cleaned content
    logger.info(f"Running batch NER on {len(prepared)} articles")
    texts = [p.content_clean for p in prepared]
    all_entities = extract_entities_batch(texts, model_key=config.spacy_model)

    # Second pass: finalize articles with entities
    articles: list[NormalizedArticle] = []
    for i, prep in enumerate(prepared):
        if prep.error:
            articles.append(make_failed_article(raw_articles[i], prep.error, ner_model))
        else:
            articles.append(finalize_article(prep, all_entities[i], ner_model))

    # Write output with timestamped filename
    output_path = output_dir / f"normalized_{run_timestamp}.{config.output_format}"
    write_output(articles, str(output_path))
    logger.info(f"Wrote {len(articles)} articles to {output_path}")


def _run_s3(config: NormalizeConfig) -> None:
    """Run normalization with S3 input (output to S3 or local)."""
    from pathlib import Path

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info(f"Starting normalization for period {config.period}")
    logger.info(f"Looking for files in s3://{config.bucket}/{config.input_prefix}")

    # List all input files for the period
    input_files = list_jsonl_files(config.bucket, config.input_prefix)

    if not input_files:
        logger.warning(f"No files found for period {config.period}")
        return

    logger.info(f"Found {len(input_files)} files for period {config.period}")

    # Resolve model key to full model name
    ner_model = SPACY_MODELS[config.spacy_model]

    # First pass: prepare all articles from all files
    prepared: list[PreparedArticle] = []
    raw_articles: list[dict] = []

    for file_path in input_files:
        logger.info(f"Reading {file_path}")
        for raw in read_jsonl(file_path):
            raw_articles.append(raw)
            try:
                prep = prepare_article(raw)
                prepared.append(prep)
                logger.debug(f"Prepared {prep.article_id}")
            except Exception as e:
                logger.error(f"Failed to prepare article {raw.get('article_id', 'unknown')}: {e}")
                prepared.append(
                    PreparedArticle(
                        article_id=raw.get("article_id", "unknown"),
                        source=raw.get("source", "unknown"),
                        url=raw.get("url", ""),
                        published_at=datetime.now(timezone.utc),
                        fetched_at=datetime.now(timezone.utc),
                        headline=raw.get("headline", ""),
                        body=raw.get("body", ""),
                        content=raw.get("content"),
                        resolution=raw.get("resolution", {}),
                        content_clean=None,
                        error=str(e),
                    )
                )

    logger.info(f"Prepared {len(prepared)} articles total")

    # Batch NER on all cleaned content
    logger.info(f"Running batch NER on {len(prepared)} articles")
    texts = [p.content_clean for p in prepared]
    all_entities = extract_entities_batch(texts, model_key=config.spacy_model)

    # Second pass: finalize articles with entities
    articles: list[NormalizedArticle] = []
    for i, prep in enumerate(prepared):
        if prep.error:
            articles.append(make_failed_article(raw_articles[i], prep.error, ner_model))
        else:
            articles.append(finalize_article(prep, all_entities[i], ner_model))

    # Write output - local or S3
    if config.output_local:
        # Write to local directory
        output_dir = Path(config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(output_dir / f"normalized_{run_timestamp}.{config.output_format}")
    else:
        # Write to S3
        output_key = build_output_key(config, run_timestamp)
        output_path = f"s3://{config.bucket}/{output_key}"

    write_output(articles, output_path)
    logger.info(f"Wrote {len(articles)} articles to {output_path}")
