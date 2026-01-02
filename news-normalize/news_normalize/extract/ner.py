from collections import Counter
from typing import Optional

import spacy

from news_normalize.extract.schema import Entity

# Whitelisted spaCy models
SPACY_MODELS = {
    "sm": "en_core_web_sm",
    "lg": "en_core_web_lg",
    "trf": "en_core_web_trf",
}
DEFAULT_SPACY_MODEL = "trf"

# Lazy load spaCy models (cached by model name)
_nlp_cache: dict[str, spacy.Language] = {}

DEFAULT_BATCH_SIZE = 16

# Entity types to extract
ENTITY_TYPES = {"PERSON", "ORG", "GPE", "LOC"}


def get_nlp(model_key: str = DEFAULT_SPACY_MODEL) -> spacy.Language:
    if model_key not in SPACY_MODELS:
        raise ValueError(
            f"Invalid model key: {model_key}. Must be one of {list(SPACY_MODELS.keys())}"
        )
    model_name = SPACY_MODELS[model_key]
    if model_name not in _nlp_cache:
        _nlp_cache[model_name] = spacy.load(model_name)
    return _nlp_cache[model_name]


def _entities_from_doc(doc) -> list[Entity]:
    """Extract entities from a single spaCy doc."""
    entity_counts: Counter[tuple[str, str]] = Counter()
    for ent in doc.ents:
        if ent.label_ in ENTITY_TYPES:
            entity_counts[(ent.text, ent.label_)] += 1

    return [
        Entity(text=text, type=label, count=count)
        for (text, label), count in entity_counts.most_common()
    ]


def extract_entities(
    text: Optional[str], model_key: str = DEFAULT_SPACY_MODEL
) -> list[Entity]:
    """
    Extract named entities from text using spaCy.

    Returns entities of types: PERSON, ORG, GPE, LOC with mention counts.
    """
    if not text:
        return []

    nlp = get_nlp(model_key)
    doc = nlp(text)
    return _entities_from_doc(doc)


def extract_entities_batch(
    texts: list[Optional[str]],
    batch_size: int = DEFAULT_BATCH_SIZE,
    model_key: str = DEFAULT_SPACY_MODEL,
) -> list[list[Entity]]:
    """
    Extract named entities from multiple texts using spaCy's nlp.pipe.

    Args:
        texts: List of texts to process (None values produce empty entity lists)
        batch_size: Batch size for nlp.pipe (default 16)
        model_key: spaCy model key (sm, lg, or trf)

    Returns:
        List of entity lists, one per input text
    """
    nlp = get_nlp(model_key)

    # Track which indices have text vs None
    valid_indices = []
    valid_texts = []
    for i, text in enumerate(texts):
        if text:
            valid_indices.append(i)
            valid_texts.append(text)

    # Initialize results with empty lists
    results: list[list[Entity]] = [[] for _ in texts]

    # Process valid texts in batches
    if valid_texts:
        for i, doc in enumerate(nlp.pipe(valid_texts, batch_size=batch_size)):
            original_idx = valid_indices[i]
            results[original_idx] = _entities_from_doc(doc)

    return results
