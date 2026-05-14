"""Rank story entities by order of first appearance in the story title + summary.

Entities whose aliases (or canonical name) appear in `title + " " + summary` are
considered "key entities" and ranked 1..N by earliest match offset. Entities that
don't appear are still attached to the story but are not flagged as key.
"""

from __future__ import annotations

import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

# Aliases shorter than this are skipped to avoid spurious matches (e.g. "US", "UN"
# would otherwise hit inside other words even with word boundaries on punctuation).
MIN_ALIAS_LENGTH = 3


def _normalize(s: str) -> str:
    """Lowercase + strip diacritics for case-insensitive accent-blind matching."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _find_earliest_offset(text_norm: str, aliases: list[str]) -> int | None:
    """Return the earliest start offset of any alias in `text_norm`, or None."""
    earliest: int | None = None
    for alias in aliases:
        alias_norm = _normalize(alias).strip()
        if len(alias_norm) < MIN_ALIAS_LENGTH:
            continue
        pattern = r"\b" + re.escape(alias_norm) + r"\b"
        match = re.search(pattern, text_norm)
        if match is None:
            continue
        if earliest is None or match.start() < earliest:
            earliest = match.start()
    return earliest


def rank_entities_by_appearance(
    text: str,
    aliases_by_qid: dict[str, list[str]],
) -> dict[str, int]:
    """
    Rank entities by first appearance of any alias in `text`.

    Args:
        text: The string to scan (typically story title + " " + summary).
        aliases_by_qid: {qid: [alias, alias, ...]} including canonical name.

    Returns:
        {qid: rank} for entities whose alias matched. Rank is 1-based, ascending
        by earliest match offset. Entities with no match are absent from the
        result; callers should treat them as non-key.
    """
    text_norm = _normalize(text)

    offsets: list[tuple[str, int]] = []
    for qid, aliases in aliases_by_qid.items():
        offset = _find_earliest_offset(text_norm, aliases)
        if offset is not None:
            offsets.append((qid, offset))

    offsets.sort(key=lambda x: x[1])
    return {qid: rank for rank, (qid, _) in enumerate(offsets, start=1)}
