"""YAML configuration loader for normalization."""

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

from news_normalize.extract.ner import SPACY_MODELS

# Load .env file if it exists
load_dotenv()

# Config directory relative to this file
CONFIG_DIR = Path(__file__).parent.parent / "configs"


@dataclass
class EmbeddingConfig:
    """Configuration for text embeddings."""

    enabled: bool = False
    model: str = "minilm"
    batch_size: int = 32
    weight_headline: float = 0.3
    weight_content: float = 0.7

    def __post_init__(self) -> None:
        if self.enabled:
            # Import here to avoid circular imports and loading torch unnecessarily
            from news_normalize.extract.embeddings import EMBEDDING_MODELS

            if self.model not in EMBEDDING_MODELS:
                raise ValueError(
                    f"Invalid embedding model: {self.model}. "
                    f"Must be one of {list(EMBEDDING_MODELS.keys())}"
                )

            # Validate weights sum to 1.0
            if abs(self.weight_headline + self.weight_content - 1.0) > 0.001:
                raise ValueError(
                    f"Embedding weights must sum to 1.0, got "
                    f"{self.weight_headline} + {self.weight_content} = "
                    f"{self.weight_headline + self.weight_content}"
                )


@dataclass
class NormalizeConfig:
    storage: str  # "s3" or "local"
    spacy_model: str = "trf"
    output_format: str = "parquet"
    period: str = ""  # For S3 mode: date to process (defaults to today)

    # S3 mode fields
    bucket: str = ""

    # Local mode fields
    input_dir: str = ""
    output_dir: str = ""  # Also used in S3 mode to override output to local

    # Embeddings configuration
    embeddings: EmbeddingConfig = field(default_factory=EmbeddingConfig)

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


def _parse_embedding_config(data: dict) -> EmbeddingConfig:
    """Parse embedding configuration from YAML data."""
    emb_data = data.get("embeddings", {})

    if isinstance(emb_data, bool):
        # Simple boolean: embeddings: true/false
        return EmbeddingConfig(enabled=emb_data)

    if not isinstance(emb_data, dict):
        return EmbeddingConfig()

    return EmbeddingConfig(
        enabled=emb_data.get("enabled", False),
        model=emb_data.get("model", "minilm"),
        batch_size=emb_data.get("batch_size", 32),
        weight_headline=emb_data.get("weight_headline", 0.3),
        weight_content=emb_data.get("weight_content", 0.7),
    )


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
    embeddings = _parse_embedding_config(data)

    if storage == "s3":
        # Load bucket from environment variable
        bucket = os.getenv("S3_BUCKET", "")
        return NormalizeConfig(
            storage="s3",
            bucket=bucket,
            spacy_model=data.get("spacy_model", "trf"),
            output_format=data.get("output_format", "parquet"),
            period=data.get("period", ""),
            output_dir=data.get("output_dir", ""),  # Optional: write locally instead of S3
            embeddings=embeddings,
        )
    else:
        # Local storage mode
        return NormalizeConfig(
            storage="local",
            input_dir=data.get("input_dir", ""),
            output_dir=data.get("output_dir", ""),
            spacy_model=data.get("spacy_model", "sm"),
            output_format=data.get("output_format", "json"),
            embeddings=embeddings,
        )
