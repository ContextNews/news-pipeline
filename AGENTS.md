# AGENTS.md

## Overview

The batch pipeline behind Context News: ingest RSS articles → embed, NER, classify → resolve entities → cluster → generate stories → link stories across days. Each stage is a standalone CLI module under `src/`, orchestrated by GitHub Actions. Everything persists to the shared Postgres schema owned by `context-db`, which `context-api` then serves.

`README.md` lists every stage, its CLI flags and DB targets. This file covers conventions and the things the README gets wrong.

## Commands

```bash
poetry install --with dev,ingest_articles,compute_embeddings,extract_entities,\
cluster_articles,classify_articles,generate_stories,link_stories

poetry run pytest                                          # unit tests only
poetry run pytest -m integration -s --log-cli-level=INFO   # hits a real database
poetry run python -m <stage> [flags]
```

Dependency groups are **optional and per-stage** — a bare `poetry install` gives you `common` and nothing else. Install only the groups for the stage you're touching (spaCy, torch and sentence-transformers are heavy).

`pytest` is configured with `addopts = ["-m", "not integration"]`, so integration tests are deselected by default. They need `DATABASE_URL` (or `NEON_DB_URL_DIRECT`/`NEON_DB_URL_POOLED`, which `tests/integration/conftest.py` promotes) and write to a live database — don't run them casually.

## Stages write nothing by default

Every stage requires an explicit sink flag: `--load-db`, `--load-object-store`, `--load-local`. Without one it computes and discards. This is the safety valve for local experimentation — prefer `--load-local` when checking a stage's output, and treat `--load-db` as a production write.

`--overwrite` on a stage deletes and replaces that day's rows. Several stages default it to `true` in `config/default.yaml` (resolve, cluster, stories) — re-running a stage is therefore destructive to that stage's prior output for that date.

## Stage module layout

Every stage follows the same six-file shape:

```
src/<stage>/
  __main__.py      # `python -m <stage>` → cli.main
  cli.py           # orchestration only: parse args → load → run → sink flags
  helpers.py       # parse_<stage>_args() and stage-local helpers
  <stage>.py       # the actual transformation, pure where possible
  models.py        # dataclasses for the stage's records
```

`cli.py` is the fixed pattern: `load_dotenv()`, `setup_logging()`, parse args, load inputs via `common.db_io`, run the transformation, then three independent `if args.load_*:` blocks. Keep DB access in `common/db_io.py` and object-store access in `common/object_storage.py` rather than inlining it in a stage.

Shared code lives in `src/common/`: `db_io`, `object_storage`, `local_io`, `cli_helpers` (`parse_date`, `date_to_range`, `setup_logging`), `datetime`, `hashing`, `serialization`, `utils`.

Tests mirror the source tree under `tests/unit/<stage>/`.

## Cross-repo dependencies

- `context-db` (git, `main`) — all models; schema changes belong in that repo.
- `Cronkite` (git) — used by `generate_stories` and `link_stories` for the LLM story synthesis and story-matching calls. Prompt behaviour lives in Cronkite, not here.
- `ContextNews/news-classifier` on Hugging Face — the model `classify_articles` loads, trained in `model-training`.

## Orchestration

`run_pipeline.yaml` runs `ingest → (embed ∥ extract-entities ∥ classify) → resolve-entities → cluster → generate-stories`. A `load-config` job parses a YAML profile from `config/` into per-stage job outputs, so **adding a stage flag means adding it in three places**: the stage's `helpers.py` parser, the profile YAML, and the workflow's output plumbing.

- Schedule is `0 9,12,15 * * *` UTC — three runs a day. The README's "06:00 and 18:00" is stale.
- `workflow_dispatch` takes `config` (profile name, default `default`) and `runner` (`github` → `ubuntu-latest`, `aws` → the self-hosted EC2 runner provisioned in `context-infra`). Every stage workflow has the same `runner` input.
- Standalone, manual-only: `link_stories.yaml`, `enrich_entities.yaml`. Scheduled: `purge.yaml` daily at 02:00 UTC.
- Each stage also has its own dispatchable workflow for re-running one step of a bad day.

**These workflows write to the production database and spend OpenAI credits.** Don't dispatch one without the user asking, and prefer `purge --dry-run` when reasoning about retention.

`purge` deletes intermediate data (embeddings, mentions, cluster membership) outside the retention window; stories and knowledge-base tables are never touched.

## Config profiles

`config/default.yaml` is production; `config/backfill.yaml` is a backfill example. An empty `date: ""` means "today", resolved by the child workflow. Values are stringly-typed by the workflow parser (note `threshold: "0.5"`), so keep quoting consistent with the existing entries.

## Environment

`DATABASE_URL` (required by anything with `--load-db`), `OPENAI_API_KEY` (`generate_stories`, `link_stories`), `S3_BUCKET_NAME` plus standard AWS credential resolution (`--load-object-store`). Object-store keys are partitioned: `{prefix}/year=YYYY/month=MM/day=DD/{filename}.jsonl`.
