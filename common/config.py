"""Shared configuration utilities."""

import os
from pathlib import Path
from typing import TypeVar, Callable, Generic

import yaml

T = TypeVar('T')


def find_config_path(
    config_name: str | None,
    config_dir: Path,
    default_name: str = "prod",
    env_var: str | None = None,
) -> Path:
    """Find config file path, checking env var and defaults.

    Args:
        config_name: Name of config (without .yaml) or None for default
        config_dir: Directory containing config files
        default_name: Default config name if config_name is None
        env_var: Environment variable to check for config name

    Returns:
        Path to the config file

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    if config_name is None:
        config_name = os.environ.get(env_var, default_name) if env_var else default_name

    config_path = config_dir / f"{config_name}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return config_path


def load_yaml(path: Path) -> dict:
    """Load YAML file and return dict."""
    with open(path) as f:
        return yaml.safe_load(f)


class ConfigSingleton(Generic[T]):
    """Generic config singleton manager.

    Provides get/set/reset pattern for managing a global config instance.

    Example:
        >>> def load_my_config() -> MyConfig:
        ...     return MyConfig(...)
        >>> _manager = ConfigSingleton(load_my_config)
        >>> get_config = _manager.get
        >>> set_config = _manager.set
        >>> reset_config = _manager.reset
    """

    def __init__(self, loader: Callable[[], T] | None = None):
        self._config: T | None = None
        self._loader = loader

    def get(self) -> T:
        """Get the config, loading it lazily if needed."""
        if self._config is None:
            if self._loader is None:
                raise RuntimeError("No config loaded and no loader set")
            self._config = self._loader()
        return self._config

    def set(self, config: T) -> None:
        """Set the config directly."""
        self._config = config

    def reset(self) -> None:
        """Reset the config, forcing reload on next get()."""
        self._config = None
