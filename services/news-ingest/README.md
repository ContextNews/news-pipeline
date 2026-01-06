# news-ingest

Batch news ingestion service that fetches articles from RSS feeds, normalizes them to a consistent schema, and writes append-only JSONL files to S3.

## How It Works

1. Read `last_fetched_at` from Postgres for each source
2. Fetch articles from all RSS feeds published after that timestamp
3. Resolve full article content (trafilatura → readability fallback)
4. Normalize to Article schema
5. Write all articles to a single gzip-compressed JSONL file on S3
6. Update state for all sources after successful upload

Safe to re-run—duplicates are acceptable, data loss is not.

## Usage

```bash
# Install
poetry install

# Local test (no S3/Postgres required)
poetry run python ingest.py --config test

# Production
cp .env.example .env  # add credentials
poetry run python ingest.py
```

Runs automatically via GitHub Actions every 6 hours.

## Configuration

| Config | Storage | State | Use Case |
|--------|---------|-------|----------|
| `prod` | S3 | Postgres | GitHub Actions, production |
| `test` | Local files | Memory | Local development, CI |

## Output Contract

### Storage Path

```
s3://{bucket}/news-raw/year=YYYY/month=MM/day=DD/raw_articles_YYYY_MM_DD_HH_MM.jsonl.gz
```

### Article Schema

Each line in the JSONL file is a JSON object with the following structure:

```json
{
  "article_id": "a1b2c3d4e5f67890",
  "source": "bbc",
  "headline": "Article headline text",
  "body": "RSS summary/description text",
  "content": "Full resolved article text (or null if resolution failed)",
  "url": "https://example.com/article",
  "published_at": "2024-01-15T10:30:00+00:00",
  "fetched_at": "2024-01-15T12:00:00+00:00",
  "resolution": {
    "success": true,
    "method": "trafilatura",
    "error": null
  }
}
```

## License

MIT
