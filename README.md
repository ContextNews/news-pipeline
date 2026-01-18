# News Pipeline

## Overview
News Pipeline ingests RSS articles, computes embeddings, and clusters related stories. It writes raw/processed data to S3 and persists core entities in RDS (PostgreSQL). The repo is organized around standalone services with CLIs and GitHub Actions workflows, plus shared utilities under `src/common`.

## Components
- `ingest_articles`: Fetches articles from RSS sources, cleans/normalizes, and loads to S3/RDS.
- `compute_embeddings`: Pulls articles from RDS by `ingested_at` date, computes sentence-transformer embeddings, and writes to S3 and the `article_embeddings` table.
- `cluster_articles`: Pulls articles + embeddings for a date, clusters with HDBSCAN, and writes results to S3 and the `article_clusters` / `article_cluster_articles` tables.
- `common`: Shared utilities for AWS/S3 helpers, datetime parsing, hashing, and dataclass serialization.

## Actions
- `Ingest Articles` (`.github/workflows/ingest_articles.yaml`)
  - Manual trigger or reusable workflow.
  - Defaults to loading to S3 and RDS.
  - Sources default to `all` (fetches all configured sources).

- `Compute Embeddings` (`.github/workflows/compute_embeddings.yaml`)
  - Manual trigger or reusable workflow.
  - Embeds articles for a given date (defaults to today, UK time).
  - Defaults to loading to S3 and RDS.

- `Cluster Articles` (`.github/workflows/cluster_articles.yaml`)
  - Manual trigger or reusable workflow.
  - Clusters by date and embedding model using HDBSCAN.
  - Defaults to loading to S3 and RDS; overwrite is enabled by default.

- `Run Pipeline` (`.github/workflows/run_pipeline.yaml`)
  - Runs ingest -> embeddings -> clustering sequentially.
  - Scheduled at 06:00 and 18:00 UTC.
  - Exposes the full set of inputs for each stage.

## Local CLI Usage (examples)
- Ingest: `poetry run python -m ingest_articles --load-s3 --load-rds`
- Embed: `poetry run python -m compute_embeddings.cli --load-s3 --load-rds`
- Cluster: `poetry run python -m cluster_articles --load-s3 --load-rds`

## Notes
- RDS access uses an SSH tunnel (bastion) in workflows.
- Embeddings are stored in `article_embeddings`; clusters are stored in `article_clusters` and `article_cluster_articles`.
