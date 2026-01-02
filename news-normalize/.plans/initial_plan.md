# news-normalize — Implementation Plan

## Goal

Build a batch normalization service that:

- Reads raw news JSONL from S3 (or local file in test mode)
- Resolves full article text from the article URL
- Extracts structured signals:
  - cleaned article text
  - entities (NER)
  - locations
  - topics (lightweight, optional v1)
- Writes normalized data to S3 as Parquet
- Is:
  - append-only
  - rerunnable
  - testable locally
  - automated via GitHub Actions

## Non-Goals (v1)

- No real-time processing
- No vector DB
- No perfect paywall handling
- No ontology / knowledge graph
- No LLM dependency

## Tech Choices (locked)

| Component            | Choice                                        |
|----------------------|-----------------------------------------------|
| Language             | Python 3.11                                   |
| Packaging            | Poetry                                        |
| NER                  | spaCy (en_core_web_sm initially)              |
| Article extraction   | trafilatura (primary), readability-lxml (fallback) |
| Storage format       | Parquet                                       |
| Parquet engine       | PyArrow                                       |
| Local tests          | pytest                                        |
| Cloud                | AWS S3                                        |
| CI                   | GitHub Actions                                |

## Repo Structure

```
news-normalize/
├── plan.md
├── pyproject.toml
├── news_normalize/
│   ├── normalize.py
│   ├── config.py
│   ├── io/
│   │   ├── read_jsonl.py
│   │   ├── write_parquet.py
│   │   └── s3.py
│   ├── resolve/
│   │   ├── resolver.py
│   │   └── extractors.py
│   ├── extract/
│   │   ├── text.py
│   │   ├── ner.py
│   │   ├── locations.py
│   │   └── schema.py
│   └── utils/
│       └── hashing.py
├── tests/
│   ├── data/
│   │   └── sample_raw.jsonl
│   └── test_normalize_local.py
└── .github/workflows/normalize.yml
```

## Input Contract (raw JSONL)

Each input row must contain:

```json
{
  "article_id": "string",
  "source": "bbc",
  "url": "https://...",
  "headline": "string",
  "published_at": "ISO-8601",
  "fetched_at": "ISO-8601"
}
```

Other fields may exist and must be ignored safely.

## Output Contract (normalized Parquet)

Each output row must contain:

```json
{
  "article_id": "string",
  "source": "string",
  "url": "string",
  "published_at": "timestamp",

  "headline": "string",
  "body": "string",

  "entities": [
    {"text": "Keir Starmer", "type": "PERSON", "count": 2}
  ],

  "locations": [
    {"name": "United Kingdom", "confidence": 0.82}
  ],

  "extraction": {
    "success": true,
    "method": "trafilatura"
  },

  "normalized_at": "timestamp",
  "normalization_version": "v1"
}
```

### Rules

- Never overwrite raw text
- Always include diagnostics
- Accept partial failures

## Processing Flow

```
JSONL → resolve article → clean text → NER → location ranking → Parquet
```

### Step-by-step

1. **Read input**
   - Local file OR `s3://bucket/raw/...`
   - Stream line-by-line (do not load whole file)

2. **Resolve article text**
   - Try in order:
     1. RSS content (if present)
     2. trafilatura.fetch_url + extract
     3. readability-lxml
   - Timeouts required
   - Fail gracefully

3. **Clean text**
   - Strip boilerplate
   - Collapse whitespace
   - Drop if word count < threshold (e.g. 150)

4. **NER**
   - spaCy pipeline
   - Extract PERSON, ORG, GPE, LOC
   - Count mentions
   - No disambiguation v1

5. **Location ranking**
   - Rank by:
     - frequency
     - presence in headline
   - Output top N (≤3)
   - Attach confidence (simple heuristic score)

6. **Write Parquet**
   - Partition by: `year / month / day / source`
   - One file per batch
   - Append-only

## Usage

### Function Signature

```python
from news_normalize.normalize import run

run(input_path: str, output_path: str) -> None
```

### Examples

```python
# Local
from news_normalize.normalize import run

run(
    input_path="tests/data/sample_raw.jsonl",
    output_path="./out/normalized.parquet"
)

# Production (S3)
run(
    input_path="s3://news-data/raw/year=2025/month=01/day=30/",
    output_path="s3://news-data/normalized/"
)
```

### Direct Script Execution

```bash
poetry run python -m news_normalize.normalize
```

When run as a script, reads config from environment variables:

- `INPUT_PATH` — input JSONL file or S3 prefix
- `OUTPUT_PATH` — output Parquet file or S3 prefix

## Local Test Requirement (MANDATORY)

`tests/test_normalize_local.py` must:

1. Read `tests/data/sample_raw.jsonl`
2. Call `run()` to execute full pipeline
3. Write Parquet to a temp directory
4. Assert:
   - row count > 0
   - body extracted OR extraction failure recorded
   - entities array exists

## Failure Handling Rules

- Never crash the batch for a single article
- Every article produces one output row
- Extraction failures are logged into the row
- Network failures are retriable (simple retry, max 2)

## GitHub Actions Workflow

### Trigger

- `push`
- `workflow_dispatch`

### Steps

1. Set up Python
2. Install Poetry deps
3. Download spaCy model
4. Run `pytest tests/test_normalize_local.py`

No AWS creds required for CI.

## Versioning

Hardcode initially:

```python
NORMALIZATION_VERSION = "v1"
```

This must be included in every output row.

## Success Criteria

The project is "done" when:

- [ ] Local test runs end-to-end
- [ ] One Parquet file is produced
- [ ] Schema matches plan
- [ ] CI is green
- [ ] Code is boring and readable

## Notes for Claude

- Do not invent features
- Do not add LLMs
- Do not over-abstract
- Prefer clarity over cleverness
- Make extraction best-effort, not perfect
