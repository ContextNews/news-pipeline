# Plan: YAML Configuration Support

## Goal

- Add YAML configuration files for normalization runs
- Create `configs/` directory with `test.yaml` and `prod.yaml`
- CLI accepts a config file path instead of individual flags

## 1. Create Config Directory Structure

```
configs/
├── test.yaml
└── prod.yaml
```

## 2. YAML Config Schema

```yaml
input: path/to/input.jsonl
output: path/to/output.json
spacy_model: trf  # sm, lg, or trf
```

## 3. Example Configs

**test.yaml**
```yaml
input: tests/data/npr_20251230_114153.jsonl
output: tests/output/test.json
spacy_model: sm
```

**prod.yaml**
```yaml
input: s3://bucket/raw/articles.jsonl
output: s3://bucket/normalized/articles.parquet
spacy_model: trf
```

## 4. Code Changes

### A. Add PyYAML dependency

Update `pyproject.toml`:
```toml
dependencies = [
    ...
    "pyyaml>=6.0",
]
```

### B. Create config loader

Create `news_normalize/config_loader.py`:
```python
import yaml
from dataclasses import dataclass

@dataclass
class NormalizeConfig:
    input: str
    output: str
    spacy_model: str = "trf"

def load_config(path: str) -> NormalizeConfig:
    with open(path) as f:
        data = yaml.safe_load(f)
    return NormalizeConfig(**data)
```

### C. Update CLI

Update `news_normalize/cli.py` to accept config:
```python
parser.add_argument("--config", required=True, help="Path to YAML config file")
```

Remove `--input`, `--output`, `--spacy-model` flags (config provides these).

## 5. Files to Create/Modify

| File | Change |
|------|--------|
| `configs/test.yaml` | Create |
| `configs/prod.yaml` | Create |
| `news_normalize/config_loader.py` | Create |
| `news_normalize/cli.py` | Update to use config |
| `pyproject.toml` | Add pyyaml dependency |
| `.github/workflows/normalize.yml` | Update to use config |
| `README.md` | Update usage docs |

## 6. Validation

- [ ] `poetry run python -m news_normalize.cli --config configs/test.yaml` works
- [ ] Config validates spacy_model is one of sm, lg, trf
- [ ] Missing required fields raise clear errors
