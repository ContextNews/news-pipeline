"""Article text resolution module."""

from news_ingest.resolve.resolver import resolve_article
from news_ingest.schema import ResolveResult

__all__ = ["resolve_article", "ResolveResult"]
