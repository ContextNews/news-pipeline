# News Pipeline

A modular, batch-oriented news processing pipeline that ingests articles from RSS feeds, enriches them with NLP processing (cleaning, embeddings, entity extraction), and loads them into PostgreSQL for downstream analysis and serving.

The pipeline is designed for correctness, re-runnability, and transparency. Each stage is isolated, deterministic, and produces explicit, versioned outputs stored in S3.

## How It Works

The pipeline consists of five sequential stages, each reading from the previous stage's S3 output:

### Stage 1: Ingest

Fetches articles from 28+ RSS feeds (BBC, CNN, Guardian, NPR, Reuters, etc.) and extracts full article text.

- Parses RSS entries for metadata (title, summary, URL, published date)
- Extracts full article text using Trafilatura with Readability fallback
- Generates deterministic article IDs from source + URL hash
- Outputs to `s3://bucket/ingested_articles/year=YYYY/month=MM/day=DD/`

### Stage 2: Clean

Cleans and normalizes text from raw articles.

- Strips HTML tags and fixes escaped characters
- Collapses whitespace and normalizes formatting
- Validates and parses datetime fields
- Outputs to `s3://bucket/cleaned_articles/year=YYYY/month=MM/day=DD/`

### Stage 3: Embed

Generates semantic embeddings for similarity-based analysis.

- Combines title + summary + article text (truncated to 250 words)
- Uses SentenceTransformer model (all-mpnet-base-v2, 768 dimensions)
- Processes in batches for efficiency
- Outputs to `s3://bucket/embedded_articles/year=YYYY/month=MM/day=DD/`

### Stage 4: Extract

Extracts named entities using spaCy transformer models.

- Identifies PERSON, ORG, GPE (geopolitical), and LOC (location) entities
- Uses en_core_web_trf model for highest accuracy
- Deduplicates entities per article
- Outputs to `s3://bucket/extracted_articles/year=YYYY/month=MM/day=DD/`

### Stage 5: Load

Loads processed articles and entities into PostgreSQL.

- Upserts articles (updates on ID conflict)
- Inserts unique entities (type + name composite key)
- Creates article-entity relationships
- All operations are idempotent for safe re-runs

## GitHub Actions

| Action  | Workflow       | Trigger                    | Description                          |
| ------- | -------------- | -------------------------- | ------------------------------------ |
| Ingest  | `ingest.yaml`  | Manual / Callable          | Fetch articles from RSS feeds        |
| Clean   | `clean.yaml`   | Manual / Callable          | Clean and normalize article text     |
| Embed   | `embed.yaml`   | Manual / Callable          | Generate semantic embeddings         |
| Extract | `extract.yaml` | Manual / Callable          | Extract named entities               |
| Run     | `run.yaml`     | Manual                     | Run full pipeline (all stages)       |
| Reset   | `reset.yaml`   | Manual (requires `RESET`)  | Delete S3 data for re-processing     |

All workflows use AWS OIDC for authentication and cache dependencies/models between runs.
