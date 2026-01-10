"""YAML configuration loader for clustering."""

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

# Config directory relative to this file
CONFIG_DIR = Path(__file__).parent.parent / "configs"


@dataclass
class ClusterConfig:
    storage: str  # "s3" or "local"
    period: str = ""  # Date to process (YYYY-MM-DD)
    window: int = 1  # Number of days to include in clustering window

    # HDBSCAN parameters
    min_cluster_size: int = 3
    min_samples: int = 2

    # Location aggregation parameters
    location_min_confidence: float = 0.65
    location_max_locations: int = 10
    location_max_regions: int = 5
    location_max_cities: int = 5

    # S3 mode fields
    bucket: str = ""

    # Local mode fields
    input_dir: str = ""
    output_dir: str = ""

    # Output format
    output_format: str = "parquet"

    def __post_init__(self) -> None:
        # Validate storage mode
        if self.storage not in ("s3", "local"):
            raise ValueError(f"Invalid storage: {self.storage}. Must be 's3' or 'local'")

        # Default period to today (UTC)
        if not self.period:
            self.period = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Validate period format
        try:
            datetime.strptime(self.period, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid period format: {self.period}. Must be YYYY-MM-DD")

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
    def date_range(self) -> list[datetime]:
        """Get list of dates to process based on period and window."""
        end_date = datetime.strptime(self.period, "%Y-%m-%d")
        dates = []
        for i in range(self.window):
            dates.append(end_date - timedelta(days=i))
        return sorted(dates)

    def input_prefix_for_date(self, date: datetime) -> str:
        """S3 prefix for input files for a specific date."""
        if self.storage != "s3":
            raise ValueError("input_prefix only available in S3 mode")
        return f"news-normalized/year={date.year}/month={date.month:02d}/day={date.day:02d}/"

    @property
    def output_prefix(self) -> str:
        """S3 prefix for output files."""
        if self.storage != "s3":
            raise ValueError("output_prefix only available in S3 mode")
        d = datetime.strptime(self.period, "%Y-%m-%d")
        return f"news-clustered/year={d.year}/month={d.month:02d}/day={d.day:02d}/"


def load_config(name: str) -> ClusterConfig:
    """Load cluster config by name (e.g., 'test' or 'prod').

    Args:
        name: Config name without extension, or full path to config file

    Returns:
        ClusterConfig instance
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

    # Get HDBSCAN parameters
    hdbscan_params = data.get("hdbscan", {})

    # Get location aggregation parameters
    location_params = data.get("location", {})

    if storage == "s3":
        bucket = os.getenv("S3_BUCKET", "")
        return ClusterConfig(
            storage="s3",
            bucket=bucket,
            period=data.get("period", ""),
            window=data.get("window", 1),
            min_cluster_size=hdbscan_params.get("min_cluster_size", 3),
            min_samples=hdbscan_params.get("min_samples", 2),
            location_min_confidence=location_params.get("min_confidence", 0.65),
            location_max_locations=location_params.get("max_locations", 10),
            location_max_regions=location_params.get("max_regions", 5),
            location_max_cities=location_params.get("max_cities", 5),
            output_format=data.get("output_format", "parquet"),
            output_dir=data.get("output_dir", ""),
        )
    else:
        return ClusterConfig(
            storage="local",
            input_dir=data.get("input_dir", ""),
            output_dir=data.get("output_dir", ""),
            period=data.get("period", ""),
            window=data.get("window", 1),
            min_cluster_size=hdbscan_params.get("min_cluster_size", 3),
            min_samples=hdbscan_params.get("min_samples", 2),
            location_min_confidence=location_params.get("min_confidence", 0.65),
            location_max_locations=location_params.get("max_locations", 10),
            location_max_regions=location_params.get("max_regions", 5),
            location_max_cities=location_params.get("max_cities", 5),
            output_format=data.get("output_format", "json"),
        )
