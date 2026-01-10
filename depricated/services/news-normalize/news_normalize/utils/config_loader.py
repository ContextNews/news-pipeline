"""YAML configuration loader for normalization."""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

from news_normalize.nlp.ner import SPACY_MODELS

# Load .env file if it exists
load_dotenv()

# Config directory relative to this file
CONFIG_DIR = Path(__file__).parent.parent.parent / "configs"


@dataclass
class NormalizeConfig:
    """Configuration for news normalization pipeline."""

    storage: str  # "s3" or "local"
    spacy_model: str = "trf"
    output_format: str = "parquet"
    period: str = ""  # For S3 mode: date to process (defaults to today)

    # S3 mode fields
    bucket: str = ""

    # Local mode fields
    input_dir: str = ""
    output_dir: str = ""  # Also used in S3 mode to override output to local

    # Embedding configuration (flattened)
    embedding_enabled: bool = False
    embedding_model: str = "minilm"
    embedding_batch_size: int = 32
    embedding_weight_headline: float = 0.3
    embedding_weight_content: float = 0.7

    # Database configuration
    database_enabled: bool = False
    database_batch_size: int = 100

    def __post_init__(self) -> None:
        # Validate storage mode
        if self.storage not in ("s3", "local"):
            raise ValueError(f"Invalid storage: {self.storage}. Must be 's3' or 'local'")

        # Default period to today (UTC) for S3 mode
        if self.storage == "s3" and not self.period:
            self.period = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Validate period format if set
        if self.period:
            try:
                datetime.strptime(self.period, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid period format: {self.period}. Must be YYYY-MM-DD")

        # Validate spacy_model
        if self.spacy_model not in SPACY_MODELS:
            raise ValueError(
                f"Invalid spacy_model: {self.spacy_model}. "
                f"Must be one of {list(SPACY_MODELS.keys())}"
            )

        # Validate output_format
        if self.output_format not in ("parquet", "json"):
            raise ValueError(
                f"Invalid output_format: {self.output_format}. Must be 'parquet' or 'json'"
            )

        # S3 mode requires bucket
        if self.storage == "s3" and not self.bucket:
            raise ValueError("S3 storage requires S3_BUCKET environment variable")

        # Local mode requires input_dir and output_dir
        if self.storage == "local":
            if not self.input_dir:
                raise ValueError("Local storage requires input_dir")
            if not self.output_dir:
                raise ValueError("Local storage requires output_dir")

        # Validate embedding config if enabled
        if self.embedding_enabled:
            from news_normalize.nlp.embeddings import EMBEDDING_MODELS

            if self.embedding_model not in EMBEDDING_MODELS:
                raise ValueError(
                    f"Invalid embedding model: {self.embedding_model}. "
                    f"Must be one of {list(EMBEDDING_MODELS.keys())}"
                )

            if abs(self.embedding_weight_headline + self.embedding_weight_content - 1.0) > 0.001:
                raise ValueError(
                    f"Embedding weights must sum to 1.0, got "
                    f"{self.embedding_weight_headline} + {self.embedding_weight_content}"
                )

    @property
    def output_local(self) -> bool:
        """Whether to write output to local directory instead of S3."""
        return bool(self.output_dir)

    @property
    def input_prefix(self) -> str:
        """S3 prefix for input files (S3 mode only)."""
        if self.storage != "s3":
            raise ValueError("input_prefix only available in S3 mode")
        d = datetime.strptime(self.period, "%Y-%m-%d")
        return f"news-raw/year={d.year}/month={d.month:02d}/day={d.day:02d}/"

    @property
    def output_prefix(self) -> str:
        """S3 prefix for output files (S3 mode only)."""
        if self.storage != "s3":
            raise ValueError("output_prefix only available in S3 mode")
        d = datetime.strptime(self.period, "%Y-%m-%d")
        return f"news-normalized/year={d.year}/month={d.month:02d}/day={d.day:02d}/"


def build_output_key(config: NormalizeConfig, run_timestamp: str) -> str:
    """Build full S3 key for output file (S3 mode only)."""
    return f"{config.output_prefix}normalized_{run_timestamp}.{config.output_format}"


def load_config(name: str) -> NormalizeConfig:
    """Load normalization config by name (e.g., 'test' or 'prod').

    Args:
        name: Config name without extension, or full path to config file

    Returns:
        NormalizeConfig instance
    """
    # Check if it's a path or a name
    if "/" in name or name.endswith(".yaml") or name.endswith(".yml"):
        config_path = Path(name)
    else:
        config_path = CONFIG_DIR / f"{name}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        data = yaml.safe_load(f)

    storage = data.get("storage", "s3")

    # Parse embedding config from nested structure
    emb_data = data.get("embeddings", {})
    if isinstance(emb_data, bool):
        embedding_enabled = emb_data
        embedding_model = "minilm"
        embedding_batch_size = 32
        embedding_weight_headline = 0.3
        embedding_weight_content = 0.7
    elif isinstance(emb_data, dict):
        embedding_enabled = emb_data.get("enabled", False)
        embedding_model = emb_data.get("model", "minilm")
        embedding_batch_size = emb_data.get("batch_size", 32)
        embedding_weight_headline = emb_data.get("weight_headline", 0.3)
        embedding_weight_content = emb_data.get("weight_content", 0.7)
    else:
        embedding_enabled = False
        embedding_model = "minilm"
        embedding_batch_size = 32
        embedding_weight_headline = 0.3
        embedding_weight_content = 0.7

    # Parse database config from nested structure
    db_data = data.get("database", {})
    if isinstance(db_data, bool):
        database_enabled = db_data
        database_batch_size = 100
    elif isinstance(db_data, dict):
        database_enabled = db_data.get("enabled", False)
        database_batch_size = db_data.get("batch_size", 100)
    else:
        database_enabled = False
        database_batch_size = 100

    if storage == "s3":
        bucket = os.getenv("S3_BUCKET", "")
        return NormalizeConfig(
            storage="s3",
            bucket=bucket,
            spacy_model=data.get("spacy_model", "trf"),
            output_format=data.get("output_format", "parquet"),
            period=data.get("period", ""),
            output_dir=data.get("output_dir", ""),
            embedding_enabled=embedding_enabled,
            embedding_model=embedding_model,
            embedding_batch_size=embedding_batch_size,
            embedding_weight_headline=embedding_weight_headline,
            embedding_weight_content=embedding_weight_content,
            database_enabled=database_enabled,
            database_batch_size=database_batch_size,
        )
    else:
        return NormalizeConfig(
            storage="local",
            input_dir=data.get("input_dir", ""),
            output_dir=data.get("output_dir", ""),
            spacy_model=data.get("spacy_model", "sm"),
            output_format=data.get("output_format", "json"),
            embedding_enabled=embedding_enabled,
            embedding_model=embedding_model,
            embedding_batch_size=embedding_batch_size,
            embedding_weight_headline=embedding_weight_headline,
            embedding_weight_content=embedding_weight_content,
            database_enabled=database_enabled,
            database_batch_size=database_batch_size,
        )
