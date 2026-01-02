"""Local pipeline test for news normalization."""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from news_normalize.normalize import run

TESTS_DIR = Path(__file__).parent
DATA_DIR = TESTS_DIR / "data"
OUTPUT_DIR = TESTS_DIR / "output"


def get_jsonl_files():
    """Discover all JSONL files in data directory."""
    return list(DATA_DIR.glob("*.jsonl"))


@pytest.fixture(scope="module", autouse=True)
def setup_output_dir():
    """Ensure output directory exists."""
    OUTPUT_DIR.mkdir(exist_ok=True)


@pytest.mark.parametrize("input_file", get_jsonl_files(), ids=lambda f: f.stem)
def test_normalize_local_pipeline(input_file):
    """Test full normalization pipeline with local files."""
    output_stem = input_file.stem
    parquet_path = OUTPUT_DIR / f"{output_stem}.parquet"
    json_path = OUTPUT_DIR / f"{output_stem}.json"

    # Run the pipeline
    run(str(input_file), str(parquet_path))

    # Assert output file exists
    assert parquet_path.exists(), "Parquet output file should be created"

    # Read and validate output
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    # Save as JSON for inspection
    records = df.to_dict(orient="records")
    for record in records:
        # Convert timestamps to ISO strings for JSON serialization
        for key in ["published_at", "normalized_at"]:
            if key in record and hasattr(record[key], "isoformat"):
                record[key] = record[key].isoformat()
        # Convert numpy arrays to lists
        for key in ["entities", "locations"]:
            if key in record and hasattr(record[key], "tolist"):
                record[key] = record[key].tolist()

    with open(json_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    # Assert row count > 0
    assert len(df) > 0, "Should have at least one output row"

    # Assert required columns exist
    required_columns = [
        # Raw fields (preserved from input)
        "article_id",
        "source",
        "url",
        "published_at",
        "fetched_at",
        "headline",
        "body",
        "content",
        "resolution",
        # Added by normalization
        "content_clean",
        "entities",
        "locations",
        "ner_model",
        "normalized_at",
    ]
    for col in required_columns:
        assert col in df.columns, f"Missing required column: {col}"

    # Assert each row has resolution info (preserved from input)
    for idx, row in df.iterrows():
        resolution = row["resolution"]
        assert "success" in resolution, f"Row {idx} missing resolution.success"

    # Assert entities array exists for each row (may be numpy array from Parquet)
    for idx, row in df.iterrows():
        entities = row["entities"]
        assert hasattr(entities, "__iter__"), f"Row {idx} entities should be iterable"

    # Assert NER model is set
    for idx, row in df.iterrows():
        assert row["ner_model"].startswith("en_core_web_"), f"Row {idx} should have valid ner_model"
