from collections import Counter
from typing import Optional

import spacy

from news_pipeline.normalize.models import Entity

SPACY_MODELS = {
    "sm": "en_core_web_sm",
    "lg": "en_core_web_lg",
    "trf": "en_core_web_trf",
}

ENTITY_TYPES = {"PERSON", "ORG", "GPE", "LOC"}

_nlp_cache: dict[str, spacy.Language] = {}


def _get_nlp(model_key: str) -> spacy.Language:
    """Get or load spaCy model."""
    if model_key not in SPACY_MODELS:
        raise ValueError(f"Invalid model: {model_key}. Must be one of {list(SPACY_MODELS.keys())}")
    model_name = SPACY_MODELS[model_key]
    if model_name not in _nlp_cache:
        _nlp_cache[model_name] = spacy.load(model_name)
    return _nlp_cache[model_name]


def _entities_from_doc(doc) -> list[Entity]:
    """Extract entities from a spaCy doc."""
    counts: Counter[tuple[str, str]] = Counter()
    for ent in doc.ents:
        if ent.label_ in ENTITY_TYPES:
            counts[(ent.text, ent.label_)] += 1
    return [Entity(text=text, type=label, count=count) for (text, label), count in counts.most_common()]


def extract_entities(text: Optional[str], model_key: str = "trf") -> list[Entity]:
    """Extract named entities from text."""
    if not text:
        return []
    nlp = _get_nlp(model_key)
    return _entities_from_doc(nlp(text))


def extract_entities_batch(
    texts: list[Optional[str]],
    model_key: str = "trf",
    batch_size: int = 16,
) -> list[list[Entity]]:
    """Extract entities from multiple texts using batch processing."""
    nlp = _get_nlp(model_key)

    # Track valid texts
    valid_indices = []
    valid_texts = []
    for i, text in enumerate(texts):
        if text:
            valid_indices.append(i)
            valid_texts.append(text)

    results: list[list[Entity]] = [[] for _ in texts]

    if valid_texts:
        for i, doc in enumerate(nlp.pipe(valid_texts, batch_size=batch_size)):
            results[valid_indices[i]] = _entities_from_doc(doc)

    return results
