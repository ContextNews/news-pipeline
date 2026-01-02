# news-normalize

Batch normalization service that reads articles from news-ingest, extracts entities via spaCy NER, and writes enriched Parquet files to S3.

## How It Works

1. List JSONL files in S3 for the target date
2. Stream and decompress articles line-by-line
3. Clean text (collapse whitespace)
4. Extract entities via spaCy NER (PERSON, ORG, GPE, LOC)
5. Rank locations by mention frequency + headline presence
6. Write Parquet to S3

Safe to re-run—every input article produces exactly one output row.

## Usage

```bash
# Install
poetry install

# Download spaCy model
poetry run python -m spacy download en_core_web_trf

# Local test (no S3 required)
poetry run python normalize.py --config test

# Production
cp .env.example .env  # add credentials
poetry run python normalize.py --config prod

# Specific date
poetry run python normalize.py --config prod --period 2024-12-30
```

Runs manually via GitHub Actions workflow dispatch.

## Configuration

| Config | Storage | SpaCy Model | Output | Use Case |
|--------|---------|-------------|--------|----------|
| `prod` | S3 | transformer | Parquet | GitHub Actions, production |
| `test` | S3 → local | small       | JSON | Local development, CI |

## Output Contract

### Storage Path

```
s3://{bucket}/news-normalized/year=YYYY/month=MM/day=DD/normalized_YYYYMMDD_HHMMSS.parquet
```

### Article Schema

Each row in the Parquet file contains:

```json
{
  "article_id": "a1b2c3d4e5f67890",
  "source": "bbc",
  "headline": "Article headline text",
  "body": "RSS summary/description text",
  "content": "Full resolved article text",
  "url": "https://example.com/article",
  "published_at": "2024-01-15T10:30:00+00:00",
  "fetched_at": "2024-01-15T12:00:00+00:00",
  "resolution": {"success": true, "method": "trafilatura", "error": null},
  "content_clean": "Cleaned article text with collapsed whitespace",
  "entities": [
    {"text": "United Nations", "type": "ORG", "count": 3},
    {"text": "John Smith", "type": "PERSON", "count": 2}
  ],
  "locations": [
    {"name": "London", "confidence": 0.85},
    {"name": "United Kingdom", "confidence": 0.62}
  ],
  "ner_model": "en_core_web_trf",
  "normalized_at": "2024-01-15T14:00:00+00:00"
}
```

## NER & Location Detection

### Entity Extraction

Uses spaCy's transformer model (`en_core_web_trf`) to extract four entity types:

| Type | Description | Example |
|------|-------------|---------|
| `PERSON` | Named individuals | "Joe Biden" |
| `ORG` | Organizations | "United Nations" |
| `GPE` | Geopolitical entities | "France", "New York" |
| `LOC` | Non-GPE locations | "Mount Everest", "Pacific Ocean" |

Entities are deduplicated and counted by mention frequency.

### Location Ranking

Locations (GPE + LOC only) are scored and the top 3 returned:

```
confidence = (normalized_frequency × 0.7) + headline_bonus
```

- **normalized_frequency**: `entity_count / max_count` across all locations
- **headline_bonus**: +0.3 if location appears in headline

## License

MIT
