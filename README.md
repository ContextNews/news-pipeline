# News Pipeline

A modular Python pipeline for ingesting news articles, enriching them, clustering related coverage, generating story summaries, and linking related stories across dates.

## Architecture

The repository contains standalone stages under `src/`:

1. `ingest_articles` - fetch RSS content, extract article text, clean records.
2. `compute_embeddings` - generate article embeddings.
3. `extract_entities` - run spaCy NER over article text.
4. `resolve_entities` - resolve GPE/PERSON entities to Wikidata-linked locations/persons.
5. `cluster_articles` - cluster related articles with HDBSCAN.
6. `generate_stories` - generate story overviews from clusters; optional topic classification and previous-day linking.
7. `link_stories` - standalone stage to link stories between any two dates.

Shared helpers live in `src/common`.

## Orchestration

### Main pipeline workflow

`Run Pipeline` (`.github/workflows/run_pipeline.yaml`) orchestrates:

`ingest -> (embed + extract-entities) -> resolve-entities -> cluster -> generate-stories`

It runs on schedule at `06:00` and `18:00` UTC, and supports manual runs with a single input:

- `config`: name of a YAML profile in `config/` (without `.yaml`), default `default`

Config profiles currently included:

- `config/default.yaml` - production defaults
- `config/backfill.yaml` - example backfill profile

### Run Pipeline flowchart

```mermaid
flowchart TD
    A[run_pipeline.yaml trigger<br/>schedule or workflow_dispatch(config)] --> B[load-config]
    B --> C[ingest]
    C --> D[embed]
    C --> E[extract-entities]
    E --> F[resolve-entities]
    D --> G[cluster]
    F --> H[generate-stories]
    G --> H
```

### Standalone linking workflow

`Link Stories` (`.github/workflows/link_stories.yaml`) is manual-only and **not** part of the main orchestrated pipeline.

Inputs:

- `date-a` (older date, required)
- `date-b` (newer date, required)
- `model` (default `gpt-4o-mini`)
- `n-candidates` (default `3`)
- `delete-existing` (default `false`)

## Setup

### Requirements

- Python `3.12`
- Poetry
- Access to the PostgreSQL database (`DATABASE_URL`)
- AWS credentials + S3 bucket for S3 writes
- OpenAI key for story generation/linking (`OPENAI_API_KEY`)

### Install dependencies

```bash
poetry install --with dev,ingest_articles,compute_embeddings,extract_entities,cluster_articles,generate_stories,link_stories
```

If you only need a subset of stages, install only the relevant dependency groups.

## Local CLI usage

Most stages only persist outputs when `--load-s3`, `--load-rds`, or `--load-local` is provided.

```bash
# Ingest
poetry run python -m ingest_articles --lookback-hours 12 --sources all --load-s3 --load-rds

# Embeddings
poetry run python -m compute_embeddings --published-date 2026-02-01 --model all-MiniLM-L6-v2 --batch-size 32 --load-s3 --load-rds

# Extract entities
poetry run python -m extract_entities --published-date 2026-02-01 --model en_core_web_trf --batch-size 32 --word-limit 300 --load-s3 --load-rds

# Resolve entities
poetry run python -m resolve_entities --published-date 2026-02-01 --load-s3 --load-rds

# Cluster
poetry run python -m cluster_articles --ingested-date 2026-02-01 --embedding-model all-MiniLM-L6-v2 --min-cluster-size 2 --min-samples 0 --load-s3 --load-rds

# Generate stories
poetry run python -m generate_stories --cluster-period 2026-02-01 --model gpt-4o-mini --classify --link-stories --load-s3 --load-rds

# Standalone story linking (between arbitrary dates)
poetry run python -m link_stories --date-a 2026-02-01 --date-b 2026-02-02 --n-candidates 3 --delete-existing --load-rds
```

## Stage outputs

Primary RDS targets written by each stage:

- `ingest_articles`: `articles`
- `compute_embeddings`: `article_embeddings`
- `extract_entities`: `entities`, `article_entities`
- `resolve_entities`: `article_locations`, `article_persons`
- `cluster_articles`: `article_clusters`, `article_cluster_articles`
- `generate_stories`: `stories`, `article_stories`, plus story metadata/link tables (for example `story_locations`, `story_persons`, `story_topics`, `story_stories`)
- `link_stories`: `story_stories`

S3 outputs use partitioned keys built as:

`{prefix}/year=YYYY/month=MM/day=DD/{filename}.jsonl`

## GitHub Actions stage workflows

- `.github/workflows/ingest_articles.yaml`
- `.github/workflows/compute_embeddings.yaml`
- `.github/workflows/extract_entities.yaml`
- `.github/workflows/resolve_entities.yaml`
- `.github/workflows/cluster_articles.yaml`
- `.github/workflows/generate_stories.yaml`
- `.github/workflows/link_stories.yaml`
- `.github/workflows/run_pipeline.yaml`

Each stage workflow sets up AWS credentials, an SSH tunnel to RDS, and runs the corresponding CLI command with built arguments.

## Tests

```bash
poetry run pytest
```
