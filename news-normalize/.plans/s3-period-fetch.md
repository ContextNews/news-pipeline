# Plan: S3 Period-Based Article Fetching

## Context

The `news-ingest` repo writes articles to S3 with this structure:
```
s3://bucket/news-raw/year=YYYY/month=MM/day=DD/source=SOURCE/*.jsonl.gz
```

Currently, news-normalize expects a single input path, which doesn't work with this dynamic structure.

## Goal

- Add `period` config setting (date to normalize, default: today)
- Automatically discover and fetch all JSONL files for that period across all sources
- Output normalized data to `news-normalized/` with matching structure

## S3 Structure & Naming Convention

### Input (from news-ingest)

Pattern: `news-raw/year=YYYY/month=MM/day=DD/source=SOURCE/articles_YYYYMMDD_HHMMSS.jsonl.gz`

```
s3://bucket/news-raw/year=2024/month=12/day=31/source=ap/articles_20241231_143045.jsonl.gz
s3://bucket/news-raw/year=2024/month=12/day=31/source=bbc/articles_20241231_143045.jsonl.gz
s3://bucket/news-raw/year=2024/month=12/day=31/source=guardian/articles_20241231_143045.jsonl.gz
...
```

### Output (from news-normalize)

Pattern: `news-normalized/year=YYYY/month=MM/day=DD/normalized_YYYYMMDD_HHMMSS.parquet`

```
s3://bucket/news-normalized/year=2024/month=12/day=31/normalized_20241231_160000.parquet
```

The timestamp in the output filename is the run timestamp (when normalization started), matching the news-ingest convention where all files from a single run share the same timestamp.

## Config Schema Changes

### Current
```yaml
input: s3://bucket/raw/articles.jsonl
output: s3://bucket/normalized/articles.parquet
spacy_model: trf
```

### New
```yaml
bucket: news-data-bucket
period: "2024-12-31"  # Optional, defaults to today
spacy_model: trf
output_format: parquet  # or json
```

When `period` is omitted, use `datetime.now().strftime("%Y-%m-%d")`.

## Implementation Steps

### 1. Update config_loader.py

```python
@dataclass
class NormalizeConfig:
    bucket: str
    spacy_model: str = "trf"
    period: str = ""  # Empty = today
    output_format: str = "parquet"

    def __post_init__(self) -> None:
        if not self.period:
            self.period = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Validate period format
        datetime.strptime(self.period, "%Y-%m-%d")

    @property
    def input_prefix(self) -> str:
        """S3 prefix for input files."""
        d = datetime.strptime(self.period, "%Y-%m-%d")
        return f"news-raw/year={d.year}/month={d.month:02d}/day={d.day:02d}/"

    @property
    def output_prefix(self) -> str:
        """S3 prefix for output files (without filename)."""
        d = datetime.strptime(self.period, "%Y-%m-%d")
        return f"news-normalized/year={d.year}/month={d.month:02d}/day={d.day:02d}/"


def build_output_key(config: NormalizeConfig, run_timestamp: str) -> str:
    """Build full S3 key for output file.

    Args:
        config: Normalization config
        run_timestamp: Timestamp string in format YYYYMMDD_HHMMSS

    Returns:
        Full S3 key, e.g.: news-normalized/year=2024/month=12/day=31/normalized_20241231_160000.parquet
    """
    return f"{config.output_prefix}normalized_{run_timestamp}.{config.output_format}"
```

### 2. Create S3 listing function

Create `news_normalize/io/s3_list.py`:

```python
def list_jsonl_files(bucket: str, prefix: str) -> list[str]:
    """List all .jsonl.gz files under an S3 prefix."""
    s3 = boto3.client("s3")
    files = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl.gz") or key.endswith(".jsonl"):
                files.append(key)

    return files
```

### 3. Update read_jsonl.py for gzip support

```python
def read_jsonl(path: str) -> Iterator[dict]:
    """Stream JSON objects from a JSONL file (local or S3, gzip or plain)."""
    if is_s3_path(path):
        content = read_s3_bytes(path)
        if path.endswith(".gz"):
            content = gzip.decompress(content)
        for line in content.decode("utf-8").splitlines():
            if line.strip():
                yield json.loads(line)
    else:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
```

### 4. Update normalize.py

Change `run()` signature:

```python
def run_from_config(config: NormalizeConfig) -> None:
    """Run normalization for a period from S3."""
    # Capture run timestamp at start (same pattern as news-ingest)
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # List all input files
    input_files = list_jsonl_files(config.bucket, config.input_prefix)

    if not input_files:
        logger.warning(f"No files found for period {config.period}")
        return

    logger.info(f"Found {len(input_files)} files for period {config.period}")

    # Read all articles from all files
    all_raw = []
    for key in input_files:
        s3_path = f"s3://{config.bucket}/{key}"
        for raw in read_jsonl(s3_path):
            all_raw.append(raw)

    # Process all articles (existing logic)
    ...

    # Write output with timestamped filename
    output_filename = f"normalized_{run_timestamp}.{config.output_format}"
    output_path = f"s3://{config.bucket}/{config.output_prefix}{output_filename}"
    write_output(articles, output_path)
```

### 5. Update CLI

```python
def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize news articles")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--period", help="Override period (YYYY-MM-DD)")

    args = parser.parse_args()
    config = load_config(args.config)

    if args.period:
        config.period = args.period

    run_from_config(config)
```

### 6. Update config files

**configs/prod.yaml:**
```yaml
bucket: your-news-bucket
spacy_model: trf
output_format: parquet
# period defaults to today
```

**configs/test.yaml:**
```yaml
bucket: your-news-bucket
period: "2024-12-30"
spacy_model: sm
output_format: json
```

## Files to Modify/Create

| File | Change |
|------|--------|
| `news_normalize/config_loader.py` | New config schema with bucket/period |
| `news_normalize/io/s3_list.py` | Create - list S3 objects |
| `news_normalize/io/s3.py` | Add `read_s3_bytes()` if missing |
| `news_normalize/io/read_jsonl.py` | Add gzip support |
| `news_normalize/normalize.py` | Add `run_from_config()`, update processing |
| `news_normalize/cli.py` | Add `--period` override flag |
| `configs/prod.yaml` | Update schema |
| `configs/test.yaml` | Update schema |
| `README.md` | Update documentation |

## Validation Checklist

- [ ] Config loads with bucket and optional period
- [ ] Period defaults to today when omitted
- [ ] S3 listing finds all .jsonl.gz files for period
- [ ] Gzip files are correctly decompressed
- [ ] All sources for a period are processed
- [ ] Output written to correct S3 path
- [ ] `--period` CLI flag overrides config
- [ ] Handles empty periods gracefully (no files found)

## Edge Cases

1. **No files for period**: Log warning, exit cleanly
2. **Mixed .jsonl and .jsonl.gz**: Handle both
3. **Large periods**: May need streaming/batching for memory
4. **Missing sources**: Process whatever exists

## Example Run

```bash
# Normalize today's articles
poetry run python -m news_normalize.cli --config configs/prod.yaml

# Normalize specific date
poetry run python -m news_normalize.cli --config configs/prod.yaml --period 2024-12-30
```
