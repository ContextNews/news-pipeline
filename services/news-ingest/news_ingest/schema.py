"""Data schemas for news-ingest service."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class IngestConfig:

    # General
    lookback_hours: int = 24
    sources: list[str] = field(default_factory=list)

    # Storage
    storage_backend: str = "s3"  # "s3" or "local"
    storage_local_path: str = "output"

    # State
    state_backend: str = "postgres"  # "postgres" or "memory"

    # Output
    output_format: str = "jsonl"  # "jsonl" or "csv"
    output_compress: bool = True

    # Resolve
    resolve_enabled: bool = True
    resolve_request_timeout: int = 30
    resolve_max_retries: int = 3

@dataclass
class ResolveResult:
    """Result of article text resolution."""
    text: Optional[str]
    method: Optional[str]
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        """Whether resolution was successful."""
        return self.text is not None
