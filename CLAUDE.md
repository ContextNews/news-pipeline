# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands

```bash
# Install dependencies
poetry install --with dev,ingest_articles,compute_embeddings,extract_entities,cluster_articles,generate_stories,link_stories

# Run tests
poetry run pytest
poetry run pytest tests/test_ingest/test_ingest.py -v  # single test file
poetry run pytest -k "test_name"  # run specific test

# Run pipeline stages (all require --load-s3 and/or --load-rds flags to persist output)
poetry run python -m ingest_articles --load-s3 --load-rds
poetry run python -m compute_embeddings --load-s3 --load-rds --published-date 2024-01-01
poetry run python -m extract_entities --load-s3 --load-rds --published-date 2024-01-01
poetry run python -m resolve_entities --load-s3 --load-rds --published-date 2024-01-01
poetry run python -m cluster_articles --load-s3 --load-rds --ingested-date 2024-01-01
poetry run python -m generate_stories --load-s3 --load-rds --cluster-period 2024-01-01
poetry run python -m link_stories --date-a 2024-01-01 --date-b 2024-01-02 --load-rds
```

## Architecture

This is a modular news processing pipeline with six sequential stages:

1. **ingest_articles** - Fetches articles from RSS feeds, extracts full text via trafilatura/readability
2. **compute_embeddings** - Generates sentence-transformer embeddings (default: all-MiniLM-L6-v2)
3. **extract_entities** - Extracts named entities using spaCy NER
4. **resolve_entities** - Resolves GPE entities to locations and PERSON entities to persons using disambiguation heuristics
5. **cluster_articles** - Groups related articles using HDBSCAN clustering on embeddings
6. **generate_stories** - Uses OpenAI (via Cronkite library) to generate story summaries from clusters and classify them by topic

Each stage is a standalone module under `src/` with its own CLI (`cli.py`), core logic, and models. Stages read from RDS (PostgreSQL) and write to both S3 and RDS.

### Key Dependencies

- **context-data-schema** (external package): Provides SQLAlchemy models and database connection via `rds_postgres.connection.get_session()` and `rds_postgres.models`
- **Cronkite** (external package): Story generation library wrapping OpenAI

### Data Flow

- Articles are identified by a 16-character SHA256 hash of their URL
- S3 paths use Hive-style partitioning: `{prefix}/year=YYYY/month=MM/day=DD/{filename}.jsonl`
- Most stages support `--overwrite` to re-process existing data

### Common Utilities (`src/common/`)

- `aws.py`: S3 upload/download, `build_s3_key()` for partitioned paths, `upload_articles()` for RDS
- `cli_helpers.py`: `parse_date()`, `date_to_range()`, `setup_logging()`
- `serialization.py`: `serialize_dataclass()` for JSONL output
- `hashing.py`: URL hashing for article IDs

### Database Access Pattern

```python
from rds_postgres.connection import get_session
from rds_postgres.models import Article

with get_session() as session:
    articles = session.query(Article).filter(...).all()
    session.commit()
```

### GitHub Actions

The pipeline runs via GitHub Actions (`.github/workflows/`). The `run_pipeline.yaml` workflow orchestrates all stages and runs on a schedule (06:00 and 18:00 UTC). Individual stage workflows can be triggered manually. RDS access uses an SSH tunnel through a bastion host.

#### Config Profiles

Pipeline settings are defined in YAML config files under `config/`. The orchestrator workflow accepts a single `config` input (the filename without `.yaml`):

- `config/default.yaml` — production defaults (used by the scheduled run)
- `config/backfill.yaml` — example backfill profile with specific dates, S3 disabled

Every setting is per-stage (no shared `overwrite` or `published-date`). To create a new profile, copy `default.yaml` and adjust values. Trigger manually with:

```
gh workflow run "Run Pipeline" -f config=backfill
```
