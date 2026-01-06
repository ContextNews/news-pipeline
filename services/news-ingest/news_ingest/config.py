"""Configuration loader for news-ingest."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class StorageConfig:
    backend: str = "s3"  # "s3" or "local"
    local_path: str = "output"


@dataclass
class StateConfig:
    backend: str = "postgres"  # "postgres" or "memory"


@dataclass
class OutputConfig:
    format: str = "jsonl"  # "jsonl" or "csv"
    compress: bool = True


@dataclass
class ResolveConfig:
    enabled: bool = True
    request_timeout: int = 30
    max_retries: int = 3


@dataclass
class Config:
    lookback_hours: int = 24
    storage: StorageConfig = field(default_factory=StorageConfig)
    state: StateConfig = field(default_factory=StateConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    resolve: ResolveConfig = field(default_factory=ResolveConfig)
    sources: list[str] = field(default_factory=list)


def load_config(config_name: str | None = None) -> Config:
    """Load configuration from YAML file.

    Args:
        config_name: Name of config file (without .yaml extension).
                    If None, uses CONFIG_ENV env var or "default".

    Returns:
        Loaded Config object
    """
    if config_name is None:
        config_name = os.environ.get("CONFIG_ENV", "prod")

    config_dir = Path(__file__).parent.parent / "configs"
    config_path = config_dir / f"{config_name}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        data = yaml.safe_load(f)

    return _parse_config(data)


def _parse_config(data: dict) -> Config:
    """Parse config dictionary into Config object."""
    storage = StorageConfig(
        backend=data.get("storage", {}).get("backend", "s3"),
        local_path=data.get("storage", {}).get("local_path", "output"),
    )

    state = StateConfig(
        backend=data.get("state", {}).get("backend", "postgres"),
    )

    output = OutputConfig(
        format=data.get("output", {}).get("format", "jsonl"),
        compress=data.get("output", {}).get("compress", True),
    )

    resolve = ResolveConfig(
        enabled=data.get("resolve", {}).get("enabled", True),
        request_timeout=data.get("resolve", {}).get("request_timeout", 30),
        max_retries=data.get("resolve", {}).get("max_retries", 3),
    )

    return Config(
        lookback_hours=data.get("lookback_hours", 24),
        storage=storage,
        state=state,
        output=output,
        resolve=resolve,
        sources=data.get("sources", []),
    )


# Global config instance (loaded on first access)
_config: Config | None = None


def get_config() -> Config:
    """Get the current configuration (lazy-loaded)."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: Config):
    """Set the global configuration (useful for testing)."""
    global _config
    _config = config


def reset_config():
    """Reset the global configuration (forces reload on next access)."""
    global _config
    _config = None
