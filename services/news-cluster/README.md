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

# Custom location aggregation thresholds
poetry run python cluster.py --config prod \
  --location-min-confidence 0.5 \
  --location-max-locations 15 \
  --location-max-regions 8 \
  --location-max-cities 8
```

Runs manually via GitHub Actions workflow dispatch.

## Configuration

| Config | Input | Clustering | Output |
|--------|-------|------------|--------|
| `prod` | S3 Parquet | HDBSCAN (mpnet) | S3 Parquet |
| `test` | Local JSON / Parquet | HDBSCAN (minilm) | Local JSON |

Key parameters (configurable in YAML or via CLI):

**HDBSCAN:**
- `min_cluster_size` - Minimum articles to form a cluster
- `min_samples` - Minimum samples for HDBSCAN core points

**Location Aggregation:**
- `location_min_confidence` - Minimum confidence to include a country (default: 0.65)
- `location_max_locations` - Maximum countries per story (default: 10)
- `location_max_regions` - Maximum regions per country (default: 5)
- `location_max_cities` - Maximum cities per country (default: 5)

**Other:**
- `window` - Number of days to include in clustering

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

## Location Determination

Story locations are determined by aggregating location data from all articles within a cluster. The system uses a hierarchical, country-based approach that preserves sub-location detail while calculating confidence scores.

### Input: Article-Level Locations

Each article arrives from `news-normalize` with pre-extracted locations:

```json
{
  "name": "London",
  "confidence": 0.92,
  "country_code": "GB",
  "type": "city",
  "parent_region": "England",
  "original": "London"
}
```

Location types: `country`, `region`, `city`, or `unknown`

### Aggregation Algorithm

For each cluster of articles:

1. **Group by Country**: All locations are bucketed by `country_code`
2. **Track Mentions**: Count occurrences at each level (country, region, city)
3. **Track Coverage**: How many articles mention each country
4. **Track Headlines**: Bonus for locations appearing in article headlines

### Confidence Scoring

Each country's confidence is calculated as:

```
confidence = mention_score + coverage_score + headline_score
```

| Component | Weight | Formula |
|-----------|--------|---------|
| `mention_score` | 60% | `min(1.0, total_mentions / 20) × 0.6` |
| `coverage_score` | 20% | `(articles_with_mentions / total_articles) × 0.2` |
| `headline_score` | 20% | `(headline_mentions / total_articles) × 0.2` |

- **Total mentions** = country mentions + region mentions + city mentions
- Only locations with `confidence >= min_confidence` are included (default: 0.65)

### Hierarchical Output Structure

Locations are structured hierarchically by country:

```json
{
  "name": "United Kingdom",
  "country_code": "GB",
  "confidence": 0.89,
  "mention_count": 28,
  "regions": [
    {"name": "England", "type": "region", "mention_count": 12},
    {"name": "Scotland", "type": "region", "mention_count": 4}
  ],
  "cities": [
    {"name": "London", "type": "city", "mention_count": 15},
    {"name": "Manchester", "type": "city", "mention_count": 6}
  ]
}
```

### Limits (Configurable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_confidence` | 0.65 | Minimum confidence to include a country |
| `max_locations` | 10 | Maximum countries per story |
| `max_regions` | 5 | Maximum regions per country |
| `max_cities` | 5 | Maximum cities per country |

These can be configured via the `location` section in YAML config or CLI flags.

### Example Flow

```
Article 1: locations = [{name: "Israel", type: "country"}, {name: "Tel Aviv", type: "city"}]
Article 2: locations = [{name: "Gaza", type: "region", country_code: "PS"}, {name: "Israel", type: "country"}]
Article 3: locations = [{name: "Jerusalem", type: "city", country_code: "IL"}]

→ Aggregation groups by country_code:
   IL (Israel): 2 country mentions + 1 city (Tel Aviv) + 1 city (Jerusalem) = 4 mentions
   PS (Palestine): 1 region (Gaza) = 1 mention

→ Confidence calculation (3 articles total):
   IL: mention_score = min(1, 4/20) × 0.6 = 0.12
       coverage = (3/3) × 0.2 = 0.2
       headline = (2/3) × 0.2 = 0.13  (assuming 2 headlines mention Israel)
       total = 0.45  ← Below default threshold (0.65), not included

   (In practice, larger clusters with more mentions easily exceed 0.65)
```

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
    {
      "name": "Israel",
      "country_code": "IL",
      "confidence": 0.95,
      "mention_count": 42,
      "regions": [],
      "cities": [
        {"name": "Tel Aviv", "type": "city", "mention_count": 8},
        {"name": "Jerusalem", "type": "city", "mention_count": 5}
      ]
    },
    {
      "name": "Palestine",
      "country_code": "PS",
      "confidence": 0.87,
      "mention_count": 25,
      "regions": [
        {"name": "Gaza Strip", "type": "region", "mention_count": 18}
      ],
      "cities": [
        {"name": "Gaza City", "type": "city", "mention_count": 12}
      ]
    }
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
