"""Configuration loader for news-api."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


@dataclass
class S3Config:
    bucket: str
    endpoint: str
    normalized_prefix: str = "news-normalized"
    clustered_prefix: str = "news-clustered"
    raw_prefix: str = "news-raw"


@dataclass
class LocalConfig:
    normalized_path: str = "tests/data/normalized"
    clustered_path: str = "tests/data/clustered"
    raw_path: str = "tests/data/raw"


@dataclass
class CacheConfig:
    enabled: bool = True
    ttl_seconds: int = 300


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8000


@dataclass
class APIConfig:
    storage: str  # "s3" or "local"
    s3: S3Config | None = None
    local: LocalConfig | None = None
    cache: CacheConfig = field(default_factory=CacheConfig)
    server: ServerConfig = field(default_factory=ServerConfig)

    @property
    def is_s3(self) -> bool:
        return self.storage == "s3"


def load_config(config_name: str = "prod") -> APIConfig:
    """Load configuration from YAML file.

    Args:
        config_name: Name of config file (without .yaml extension)

    Returns:
        APIConfig instance
    """
    # Find config file
    config_dir = Path(__file__).resolve().parent.parent / "configs"
    config_path = config_dir / f"{config_name}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    storage = raw.get("storage", "s3")

    # Build S3 config from env vars + yaml
    s3_config = None
    if storage == "s3":
        s3_raw = raw.get("s3", {})
        s3_config = S3Config(
            bucket=os.getenv("S3_BUCKET", "news-raw"),
            endpoint=os.getenv("S3_ENDPOINT", "https://s3.amazonaws.com"),
            normalized_prefix=s3_raw.get("normalized_prefix", "news-normalized"),
            clustered_prefix=s3_raw.get("clustered_prefix", "news-clustered"),
            raw_prefix=s3_raw.get("raw_prefix", "news-raw"),
        )

    # Build local config
    local_config = None
    if storage == "local":
        local_raw = raw.get("local", {})
        local_config = LocalConfig(
            normalized_path=local_raw.get("normalized_path", "tests/data/normalized"),
            clustered_path=local_raw.get("clustered_path", "tests/data/clustered"),
            raw_path=local_raw.get("raw_path", "tests/data/raw"),
        )

    # Build cache config
    cache_raw = raw.get("cache", {})
    cache_config = CacheConfig(
        enabled=cache_raw.get("enabled", True),
        ttl_seconds=cache_raw.get("ttl_seconds", 300),
    )

    # Build server config
    server_raw = raw.get("server", {})
    server_config = ServerConfig(
        host=server_raw.get("host", "0.0.0.0"),
        port=server_raw.get("port", 8000),
    )

    return APIConfig(
        storage=storage,
        s3=s3_config,
        local=local_config,
        cache=cache_config,
        server=server_config,
    )


# Global config instance (lazy loaded)
_config: APIConfig | None = None


def get_config() -> APIConfig:
    """Get the global config instance."""
    global _config
    if _config is None:
        config_name = os.getenv("NEWS_API_CONFIG", "prod")
        _config = load_config(config_name)
    return _config


def set_config(config: APIConfig) -> None:
    """Set the global config instance (for testing)."""
    global _config
    _config = config
