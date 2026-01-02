# Plan: Make spaCy Model Configurable + Cache in GitHub Actions

## Goal

- Make the spaCy NER model configurable at runtime
- Default to `en_core_web_trf`
- Ensure GitHub Actions caches downloaded spaCy models
- No unnecessary CI complexity

---

## 1. Code Changes (Python)

### A. Add whitelisted model mapping

Location: `news_normalize/extract/ner.py` (near existing NER code)

```python
SPACY_MODELS = {
    "sm": "en_core_web_sm",
    "lg": "en_core_web_lg",
    "trf": "en_core_web_trf",
}
DEFAULT_SPACY_MODEL = "trf"
```

Only these three keys are allowed.

### B. Update spaCy loader

Refactor the existing spaCy loading to:

```python
_nlp_cache: dict[str, spacy.Language] = {}

def get_nlp(model_key: str = DEFAULT_SPACY_MODEL) -> spacy.Language:
    if model_key not in SPACY_MODELS:
        raise ValueError(f"Invalid model key: {model_key}. Must be one of {list(SPACY_MODELS.keys())}")
    model_name = SPACY_MODELS[model_key]
    if model_name not in _nlp_cache:
        _nlp_cache[model_name] = spacy.load(model_name)
    return _nlp_cache[model_name]
```

- Lazy load with in-memory cache
- Keyed by full model name

### C. Create CLI entry point

Create `news_normalize/cli.py`:

```python
import argparse
from news_normalize.normalize import run
from news_normalize.extract.ner import SPACY_MODELS, DEFAULT_SPACY_MODEL

def main():
    parser = argparse.ArgumentParser(description="Normalize news articles")
    parser.add_argument("--input", required=True, help="Input JSONL path")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument(
        "--spacy-model",
        choices=list(SPACY_MODELS.keys()),
        default=DEFAULT_SPACY_MODEL,
        help=f"spaCy model to use (default: {DEFAULT_SPACY_MODEL})"
    )
    args = parser.parse_args()
    run(args.input, args.output, spacy_model=args.spacy_model)

if __name__ == "__main__":
    main()
```

### D. Update normalize.py

Modify `run()` signature to accept `spacy_model` parameter and pass it through to NER extraction.

### E. Verify batching

- Confirm `nlp.pipe` is still used for transformer model
- No changes to batch size or extraction logic

---

## 2. Dependencies

Verify `pyproject.toml` includes:

```toml
spacy = "^3.7"
spacy-transformers = "^1.3"
```

Models are downloaded at runtime, not vendored.

---

## 3. GitHub Actions Workflow

Update `.github/workflows/normalize.yml`:

### Structure

```yaml
name: Normalize Test

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  SPACY_MODEL: trf

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Poetry
        uses: snok/install-poetry@v1

      - name: Install dependencies
        run: poetry install --all-extras

      - name: Cache spaCy models
        uses: actions/cache@v4
        with:
          path: ~/.cache/spacy
          key: spacy-${{ runner.os }}-${{ env.SPACY_MODEL }}

      - name: Download spaCy model
        run: poetry run python -m spacy download en_core_web_trf

      - name: Run local normalize test
        run: |
          poetry run python -m news_normalize.cli \
            --input tests/data/sample_raw.jsonl \
            --output ./out \
            --spacy-model trf
```

---

## 4. Files to Modify

| File | Change |
|------|--------|
| `news_normalize/extract/ner.py` | Add model mapping, refactor loader |
| `news_normalize/normalize.py` | Accept `spacy_model` param, pass through |
| `news_normalize/cli.py` | Create new CLI entry point |
| `.github/workflows/normalize.yml` | Add caching and smoke test |
| `pyproject.toml` | Verify deps, add CLI entry point |

---

## 5. Validation Checklist

- [ ] spaCy model is configurable via `--spacy-model` CLI flag
- [ ] Default model is `trf`
- [ ] Only `sm`, `lg`, `trf` are valid choices
- [ ] Invalid model key raises clear error
- [ ] spaCy model cache is reused between GH runs
- [ ] First GH run downloads model, subsequent runs skip download
- [ ] `nlp.pipe` batching still works
- [ ] No unrelated refactors

---

## 6. Non-Goals (explicitly avoided)

- Dynamic/arbitrary model names
- GPU logic
- Multiple workflows
- Environment-variable-based config
- Output schema changes
- Config files

---

## Success Criteria

1. `poetry run python -m news_normalize.cli --input X --output Y --spacy-model sm` works
2. Default (`--spacy-model` omitted) uses `trf`
3. GitHub Actions workflow passes
4. Second GH run reuses cached model
5. Code remains simple
