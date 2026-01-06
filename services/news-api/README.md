# News API

FastAPI service for accessing normalized news articles and clustered stories from S3.

## Running

```bash
# Install dependencies
poetry install --with api

# Run with prod config (reads from S3)
NEWS_API_CONFIG=prod poetry run python -m news_api.main

# Run with test config (reads from local files)
NEWS_API_CONFIG=test poetry run python -m news_api.main
```

Or using uvicorn directly:

```bash
NEWS_API_CONFIG=prod poetry run uvicorn news_api.main:app --host 0.0.0.0 --port 8000
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/articles` | List articles |
| GET | `/articles/{id}` | Get single article |
| GET | `/stories` | List stories |
| GET | `/stories/{id}` | Get single story |
| GET | `/stories/{id}/articles` | Get articles in a story |

## Query Parameters

### `/articles`

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `date` | YYYY-MM-DD | today | Date to query |
| `source` | string | - | Filter by source |
| `limit` | int | 50 | Max results (1-200) |
| `offset` | int | 0 | Pagination offset |
| `include_embeddings` | bool | false | Include embedding vectors |

### `/stories`

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `date` | YYYY-MM-DD | today | Date to query |
| `min_articles` | int | 1 | Minimum article count |
| `source` | string | - | Filter by source |
| `country` | string | - | Filter by country code (US, GB, etc.) |
| `limit` | int | 50 | Max results (1-200) |
| `offset` | int | 0 | Pagination offset |

## Configuration

### Environment Variables

```
S3_BUCKET=news-raw
S3_ENDPOINT=https://s3.amazonaws.com
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
NEWS_API_CONFIG=prod  # or test
```

## API Documentation

Once running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
