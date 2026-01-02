"""Source registry."""

from news_ingest.sources import (
    ap,
    bbc,
    cnn,
    fox,
    ft,
    guardian,
    npr,
    nyt,
    politico,
    reuters,
    sky,
    telegraph,
    wapo,
    yahoo,
)

# Registry mapping source IDs to their modules
SOURCES = {
    "ap": ap,
    "bbc": bbc,
    "cnn": cnn,
    "fox": fox,
    "ft": ft,
    "guardian": guardian,
    "npr": npr,
    "nyt": nyt,
    "politico": politico,
    "reuters": reuters,
    "sky": sky,
    "telegraph": telegraph,
    "wapo": wapo,
    "yahoo": yahoo,
}


def get_source_module(source_id: str):
    """Get the source module for a given source ID."""
    if source_id not in SOURCES:
        raise ValueError(f"Unknown source: {source_id}. Valid sources: {list(SOURCES.keys())}")
    return SOURCES[source_id]
