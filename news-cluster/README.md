# news-cluster

Batch clustering service that groups normalized news articles into stories using semantic embeddings and density-based clustering.

Consumes Parquet output from news-normalize, assigns each article to a story, and writes story + article mappings back to S3.

## How It Works

1. Read normalized Parquet files for the target date range
2. Load `embedding_combined` vectors
3. L2-normalize embeddings (cosine space)
4. Cluster articles using HDBSCAN (no fixed K)
5. Assign a `story_id` to each article
6. Aggregate cluster metadata into story records
7. Write story and article-story mappings to S3

Safe to re-run—clustering is deterministic for a fixed input set.

## Usage

```bash
# Install
poetry install

# Local test (small sample, local files)
poetry run python cluster.py --config test

# Production
cp .env.example .env  # add credentials
poetry run python cluster.py --config prod

# Specific date
poetry run python cluster.py --config prod --period 2024-12-30

# Rolling window (e.g. last 3 days)
poetry run python cluster.py --config prod --window 3
```

Runs manually via GitHub Actions workflow dispatch.

## Configuration

| Config | Input | Clustering | Output |
|--------|-------|------------|--------|
| `prod` | S3 Parquet | HDBSCAN (mpnet) | S3 Parquet |
| `test` | Local JSON / Parquet | HDBSCAN (minilm) | Local JSON |

Key parameters (configurable):

- `min_cluster_size`
- `min_samples`
- `metric` (cosine)
- `time_window_days`

## Clustering Strategy

### Embeddings

- Uses `embedding_combined` from news-normalize
- Vectors are L2-normalized before clustering
- Distance metric: cosine

### Algorithm

Uses HDBSCAN because it:

- Does not require specifying number of stories
- Handles noise / one-off articles naturally
- Works well in high-dimensional embedding space

Articles assigned cluster `-1` are treated as unclustered (no story).

## Output Contract

### Storage Paths

```
s3://{bucket}/news-clustered/year=YYYY/month=MM/day=DD/
├── stories_YYYYMMDD_HHMMSS.parquet
└── article_story_map_YYYYMMDD_HHMMSS.parquet
```

### Article → Story Mapping

Each row maps one article to a story:

```json
{
  "article_id": "a1b2c3d4e5f67890",
  "story_id": "story_3f9c1a2b",
  "cluster_label": 12,
  "assigned_at": "2024-01-15T15:10:00+00:00"
}
```

- `cluster_label = -1` → unclustered article
- `story_id` is stable for the clustering run

### Story Schema

Each row represents one clustered story:

```json
{
  "story_id": "story_3f9c1a2b",
  "title": "UN calls emergency meeting over Gaza ceasefire",
  "article_count": 14,
  "sources": ["bbc", "reuters", "guardian"],
  "top_entities": [
    {"text": "United Nations", "type": "ORG", "count": 18},
    {"text": "Gaza", "type": "GPE", "count": 11}
  ],
  "locations": [
    {"name": "Gaza", "confidence": 0.91}
  ],
  "story_embedding": [0.021, -0.034, ...],
  "start_published_at": "2024-01-15T08:12:00+00:00",
  "end_published_at": "2024-01-15T14:55:00+00:00",
  "created_at": "2024-01-15T15:10:00+00:00"
}
```

### Story Title Selection

1. Compute cluster centroid
2. Choose headline closest to centroid (cosine similarity)
3. Falls back to earliest headline if needed

## License

MIT
