# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands

```bash
# Install dependencies (Python 3.12+, Poetry)
poetry install --with dev,ingest_articles,compute_embeddings,extract_entities,cluster_articles,generate_stories,link_stories,classify_articles

# Run tests
poetry run pytest
poetry run pytest tests/unit/ingest_articles/test_ingest_articles.py -v  # single test file
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

This is a modular news processing pipeline with seven stages:

1. **ingest_articles** - Fetches articles from RSS feeds, extracts full text via trafilatura/readability
2. **compute_embeddings** - Generates sentence-transformer embeddings (default: all-MiniLM-L6-v2)
3. **extract_entities** - Extracts named entities using spaCy NER
4. **resolve_entities** - Resolves GPE entities to locations and PERSON entities to persons using disambiguation heuristics
5. **cluster_articles** - Groups related articles using HDBSCAN clustering on embeddings
6. **generate_stories** - Uses OpenAI (via Cronkite library) to generate story summaries from clusters and classify them by topic
7. **link_stories** - Standalone stage to link stories between any two dates using embedding similarity + LLM confirmation

**classify_articles** is a library module (no CLI) used by other stages to classify articles by topic using a HuggingFace text-classification model (default: `ContextNews/news-classifier`). Returns `ClassifiedArticle` objects with topic labels and sigmoid scores.

Orchestration order: `ingest → (embed + extract_entities in parallel) → resolve_entities → cluster → generate_stories`. The `link_stories` stage runs independently.

### Module Structure

Each stage is a standalone module under `src/` following the same pattern:
- `__main__.py` — entry point, delegates to `cli.main()`
- `cli.py` — argument parsing, output persistence (S3/RDS/local), orchestration
- `{stage_name}.py` — core logic (pure functions, no I/O)
- `models.py` — dataclasses for stage inputs/outputs
- `helpers.py` — argument parsing and stage-specific utilities

### Import Convention

`pythonpath = [".", "src"]` in pyproject.toml enables direct module imports without `src.` prefix:

```python
from ingest_articles.ingest_articles import ingest_articles
from common.aws import upload_jsonl_records_to_s3
from common.cli_helpers import parse_date, date_to_range, setup_logging
```

### Key Dependencies

- **context-data-schema** (external package): Provides SQLAlchemy models and database connection via `rds_postgres.connection.get_session()` and `rds_postgres.models`
- **Cronkite** (external package): Story generation library wrapping OpenAI. Constructor: `Cronkite(model, config)`

### Data Flow

- Articles are identified by a 16-character SHA256 hash of their URL
- S3 paths use Hive-style partitioning: `{prefix}/year=YYYY/month=MM/day=DD/{filename}.jsonl`
- Most stages support `--overwrite` to re-process existing data
- Stages persist output only when `--load-s3`, `--load-rds`, or `--load-local` flags are provided

### Common Utilities (`src/common/`)

- `aws.py`: S3 upload/download, `build_s3_key()` for partitioned paths, all RDS load/upload functions
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

### Testing Conventions

Tests live in `tests/unit/` mirroring the stage structure (e.g., `tests/unit/ingest_articles/test_ingest_articles.py`). Key patterns:

- **Import paths** match source module paths: `from ingest_articles.clean_articles.clean import clean`
- **Mock paths** use the importing module's namespace: `@patch("ingest_articles.ingest_articles.fetch_articles")`
- **Test classes** named per function/group: `class TestIngestArticles:`
- **Integration tests** marked with `@pytest.mark.integration` (excluded from default runs)

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
