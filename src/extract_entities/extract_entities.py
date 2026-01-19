"""Extract named entities from article text using spaCy."""

from __future__ import annotations

import logging
import re
from typing import Any

import pycountry
import spacy

from extract_entities.models import ArticleEntity

logger = logging.getLogger(__name__)


def _get_value(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _apply_word_limit(text: str, word_limit: int | None) -> str:
    if not word_limit or not text:
        return text
    words = text.split()
    if len(words) <= word_limit:
        return text
    return " ".join(words[:word_limit])


def _normalize_entity_name(text: str) -> str:
    entity_name = text.replace("\n", " ").strip().upper()
    if entity_name.endswith("&APOS;S"):
        entity_name = entity_name[:-7]
    if entity_name.endswith("'S") or entity_name.endswith("S'") or entity_name.endswith("’S") or entity_name.endswith("S’"):
        entity_name = entity_name[:-2]
    entity_name = re.sub(r"[^\w]+$", "", entity_name).strip()
    return entity_name


def _contains_alias(short_name: str, long_name: str) -> bool:
    if not short_name or not long_name or short_name == long_name:
        return False
    pattern = rf"(?<!\\w){re.escape(short_name)}(?!\\w)"
    return re.search(pattern, long_name) is not None


def _normalize_gpe_name(name: str) -> str:
    if not name:
        return name
    cleaned = name
    if cleaned.startswith("THE "):
        cleaned = cleaned[4:]
    cleaned = re.sub(r"[^\w\s]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _normalize_country_name(name: str) -> str | None:
    if not name:
        return None
    manual = {
        "UK": "UNITED KINGDOM",
        "BRITAIN": "UNITED KINGDOM",
    }
    if name in manual:
        return manual[name]
    for candidate in (name, name.title()):
        try:
            country = pycountry.countries.lookup(candidate)
        except LookupError:
            continue
        return country.name.upper()
    return None


def _collect_article_texts(
    articles: list[Any],
    word_limit: int | None,
) -> list[dict[str, str]]:
    rows = []
    for article in articles:
        article_id = _get_value(article, "id")
        if not article_id:
            continue
        title = _get_value(article, "title") or ""
        summary = _get_value(article, "summary") or ""
        text = _get_value(article, "text") or ""
        combined = " ".join(part for part in (title, summary, text) if part)
        combined = _apply_word_limit(combined, word_limit)
        if not combined:
            continue
        rows.append(
            {
                "id": article_id,
                "title": title,
                "summary": summary,
                "text": text,
                "combined": combined,
            }
        )
    return rows


def extract_entities(
    articles: list[Any],
    model: str,
    batch_size: int = 32,
    word_limit: int | None = 300,
) -> list[ArticleEntity]:
    """
    Extract named entities from article text.

    Args:
        articles: List of article objects/dicts with id, title, summary, text fields.
        model: spaCy model to load for NER.
        batch_size: Batch size for spaCy pipeline.
        word_limit: Maximum number of words to extract entities from (None for no limit).

    Returns:
        List of ArticleEntity rows.
    """
    if not articles:
        logger.warning("No articles to extract entities from")
        return []

    article_rows = _collect_article_texts(articles, word_limit)
    if not article_rows:
        logger.warning("No article text available for entity extraction")
        return []

    logger.info("Loading spaCy model: %s", model)
    nlp = spacy.load(model)

    results: list[ArticleEntity] = []
    total_entities = 0

    texts = [row["combined"] for row in article_rows]
    logger.info("Extracting entities from %d articles (batch_size=%d)", len(texts), batch_size)
    allowed_labels = {"GPE", "ORG", "PERSON", "NORP", "LOC"}
    for row, doc in zip(article_rows, nlp.pipe(texts, batch_size=batch_size), strict=True):
        name_order: list[str] = []
        name_first_label: dict[str, str] = {}
        name_label_counts: dict[str, dict[str, int]] = {}
        name_total_counts: dict[str, int] = {}

        for ent in doc.ents:
            if ent.label_ not in allowed_labels:
                continue
            entity_name = _normalize_entity_name(ent.text)
            if not entity_name:
                continue
            if ent.label_ == "GPE":
                entity_name = _normalize_gpe_name(entity_name)
                if not entity_name:
                    continue
                normalized_country = _normalize_country_name(entity_name)
                if normalized_country:
                    entity_name = normalized_country
            if entity_name not in name_first_label:
                name_first_label[entity_name] = ent.label_
                name_order.append(entity_name)
            name_total_counts[entity_name] = name_total_counts.get(entity_name, 0) + 1
            label_counts = name_label_counts.setdefault(entity_name, {})
            label_counts[ent.label_] = label_counts.get(ent.label_, 0) + 1

        title_names: set[str] = set()
        if row["title"]:
            title_doc = nlp(row["title"])
            for ent in title_doc.ents:
                if ent.label_ not in allowed_labels:
                    continue
                name = _normalize_entity_name(ent.text)
                if ent.label_ == "GPE":
                    name = _normalize_gpe_name(name)
                    normalized_country = _normalize_country_name(name)
                    if normalized_country:
                        name = normalized_country
                if name:
                    title_names.add(name)

        entities: list[ArticleEntity] = []
        order_index = {name: index for index, name in enumerate(name_order)}
        for entity_name in name_order:
            label_counts = name_label_counts[entity_name]
            max_count = max(label_counts.values())
            labels_with_max = [label for label, count in label_counts.items() if count == max_count]
            if len(labels_with_max) == 1:
                chosen_label = labels_with_max[0]
            else:
                chosen_label = name_first_label[entity_name]
            entities.append(
                ArticleEntity(
                    article_id=row["id"],
                    entity_type=chosen_label,
                    entity_name=entity_name,
                    in_title=entity_name in title_names,
                    count=name_total_counts[entity_name],
                    aliases=None,
                )
            )

        name_to_entity = {entity.entity_name: entity for entity in entities}
        person_names = [entity.entity_name for entity in entities if entity.entity_type == "PERSON"]
        to_remove: set[str] = set()
        for short_name in person_names:
            candidates = [name for name in person_names if _contains_alias(short_name, name)]
            if not candidates:
                continue
            chosen_name = max(candidates, key=lambda name: (len(name), -order_index[name]))
            long_entity = name_to_entity[chosen_name]
            short_entity = name_to_entity[short_name]
            long_entity.count += short_entity.count
            if long_entity.aliases is None:
                long_entity.aliases = []
            long_entity.aliases.append(short_entity.entity_name)
            if short_entity.aliases:
                long_entity.aliases.extend(short_entity.aliases)
            to_remove.add(short_name)

        filtered_entities = []
        for entity in entities:
            if entity.entity_name in to_remove:
                continue
            if entity.aliases:
                entity.aliases = sorted(set(entity.aliases))
            else:
                entity.aliases = None
            filtered_entities.append(entity)

        results.extend(filtered_entities)
        total_entities += len(filtered_entities)

    logger.info("Extracted %d unique entities from %d articles", total_entities, len(texts))
    return results
