# news-normalize

Batch normalization service that reads articles from news-ingest, extracts entities via spaCy NER, generates text embeddings, and writes enriched Parquet files to S3.

## How It Works

1. List JSONL files in S3 for the target date
2. Stream and decompress articles line-by-line
3. Clean text (collapse whitespace)
4. Extract entities via spaCy NER (PERSON, ORG, GPE, LOC)
5. Resolve GPE entities to countries (via pycountry + Nominatim geocoding)
6. Group locations by country with sub-entities (cities, regions)
7. Score locations by frequency + headline presence
8. Generate embeddings for headline, content, and combined
9. Write Parquet to S3

Safe to re-run—every input article produces exactly one output row.

## Usage

```bash
# Install
poetry install

# Download models
poetry run python -m spacy download en_core_web_trf
poetry run python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-mpnet-base-v2')"

# Local test (no S3 required)
poetry run python -m news_normalize.cli --config test

# Production
cp .env.example .env  # add credentials
poetry run python -m news_normalize.cli --config prod

# Specific date
poetry run python -m news_normalize.cli --config prod --period 2024-12-30
```

Runs manually via GitHub Actions workflow dispatch.

## Configuration

| Config | Storage | SpaCy Model | Embedding Model | Output |
|--------|---------|-------------|-----------------|--------|
| `prod` | S3 | transformer | mpnet (768-dim) | Parquet |
| `test` | S3 → local | small | minilm (384-dim) | JSON |

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
    {
      "name": "United Kingdom",
      "country_code": "GB",
      "count": 5,
      "in_headline": true,
      "confidence": 0.85,
      "sub_entities": [
        {"name": "London", "count": 2, "in_headline": false},
        {"name": "Scotland", "count": 1, "in_headline": false}
      ]
    },
    {
      "name": "United States",
      "country_code": "US",
      "count": 3,
      "in_headline": false,
      "confidence": 0.72,
      "sub_entities": [
        {"name": "New York", "count": 1, "in_headline": false}
      ]
    }
  ],
  "ner_model": "en_core_web_trf",
  "embedding_headline": [0.023, -0.041, ...],
  "embedding_content": [0.018, -0.033, ...],
  "embedding_combined": [0.019, -0.035, ...],
  "embedding_model": "sentence-transformers/all-mpnet-base-v2",
  "embedding_dim": 768,
  "embedding_chunks": 3,
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

### Location Identification & Structuring

Locations are extracted from GPE (geopolitical entities) only. Each GPE is resolved to its parent country, with cities and regions nested as sub-entities.

#### Location Schema

Each location is a country with optional sub-entities:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Canonical country name (e.g., "United Kingdom") |
| `country_code` | string | ISO 3166-1 alpha-2 code (e.g., "GB", "US") |
| `count` | int | Total mentions (country + all sub-entities) |
| `in_headline` | bool | True if country or any sub-entity in headline |
| `confidence` | float | Score from 0-1 based on frequency and headline presence |
| `sub_entities` | array | Cities/regions within this country |

Sub-entities have:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Entity name as extracted (e.g., "London", "California") |
| `count` | int | Number of mentions |
| `in_headline` | bool | True if entity appears in headline |

#### GPE Resolution

GPE entities are resolved to countries using a three-step process:

1. **Country detection** — Check if entity is a country via pycountry + custom aliases
2. **Subdivision lookup** — Check pycountry.subdivisions for regions/states/provinces
3. **Geocoding fallback** — Use Nominatim (OpenStreetMap) for cities

Results are cached in-memory for performance.

#### Country Normalization

Country names are normalized to canonical forms with ISO codes:

| Variation Type | Examples | Normalized To |
|---------------|----------|---------------|
| Abbreviations | "UK", "U.K.", "US", "U.S.A." | "United Kingdom" (GB), "United States" (US) |
| Informal names | "Britain", "America", "Holland" | "United Kingdom" (GB), "United States" (US), "Netherlands" (NL) |
| Historical names | "Burma", "Soviet Union" | "Myanmar" (MM), "Russia" (RU) |
| Constituent parts | "England", "Scotland", "Wales" | "United Kingdom" (GB) |

#### Confidence Scoring

All locations are returned, sorted by confidence:

```
confidence = (normalized_frequency × 0.7) + headline_bonus
```

- **normalized_frequency**: `location.count / max_count` across all locations
- **headline_bonus**: +0.3 if country or any sub-entity appears in headline

#### Example Output

For an article about US-Denmark tensions over Greenland with mentions of Copenhagen and Washington:

```json
"locations": [
  {
    "name": "Greenland",
    "country_code": "GL",
    "count": 5,
    "in_headline": true,
    "confidence": 0.85,
    "sub_entities": []
  },
  {
    "name": "United States",
    "country_code": "US",
    "count": 4,
    "in_headline": true,
    "confidence": 0.76,
    "sub_entities": [
      {"name": "Washington", "count": 1, "in_headline": false}
    ]
  },
  {
    "name": "Denmark",
    "country_code": "DK",
    "count": 3,
    "in_headline": false,
    "confidence": 0.42,
    "sub_entities": [
      {"name": "Copenhagen", "count": 1, "in_headline": false}
    ]
  }
]
```

## Embeddings

### Models

| Key | Model | Dimensions | Use Case |
|-----|-------|------------|----------|
| `mpnet` | all-mpnet-base-v2 | 768 | Production (higher quality) |
| `minilm` | all-MiniLM-L6-v2 | 384 | Testing (faster) |

### Vectors

Three embeddings per article:

- **headline**: Embedded directly
- **content**: Long text chunked at sentence boundaries, mean-pooled
- **combined**: Weighted combination (headline 0.3 + content 0.7), normalized

### Chunking

Articles exceeding the model's token limit are split into chunks at sentence boundaries. The `embedding_chunks` field records how many chunks were used.

## License

MIT
