import logging
from datetime import date

import numpy as np

logger = logging.getLogger(__name__)


def get_similar_stories(
    story_id: str,
    target_date: date,
    n: int,
    embedding_model: str = "all-MiniLM-L6-v2",
) -> list[dict]:
    """Return the n stories from target_date most similar to story_id.

    Each result dict contains story_id, title, summary, similarity_score,
    and breakdown scores (embedding_similarity, topic_similarity, entity_similarity).
    """
    from rds_postgres.connection import get_session

    with get_session() as session:
        # Load input story metadata
        input_stories = _load_stories_with_metadata(session, target_date, exclude_story_id=None)
        input_story = next((s for s in input_stories if s["story_id"] == story_id), None)
        if input_story is None:
            # Story not on target_date — load it directly
            input_story = _load_single_story_metadata(session, story_id)
            if input_story is None:
                return []

        # Load candidates (stories on target_date excluding input)
        candidates = _load_stories_with_metadata(session, target_date, exclude_story_id=story_id)
        if not candidates:
            logger.info("No candidate stories found on %s for story %s", target_date, story_id)
            return []

        logger.info(
            "Found %d candidate stories on %s to compare against '%s'",
            len(candidates), target_date, input_story["title"],
        )

        # Load embeddings for input + candidates
        all_story_ids = [story_id] + [c["story_id"] for c in candidates]
        embeddings_by_story = _load_story_embeddings(session, all_story_ids, embedding_model)

        input_embedding = _compute_mean_embedding(embeddings_by_story.get(story_id, []))

    # Score each candidate
    scored = []
    for candidate in candidates:
        cid = candidate["story_id"]
        candidate_embedding = _compute_mean_embedding(embeddings_by_story.get(cid, []))

        if input_embedding is not None and candidate_embedding is not None:
            emb_sim = _cosine_similarity(input_embedding, candidate_embedding)
        else:
            emb_sim = 0.0

        topic_sim = _jaccard_similarity(input_story["topics"], candidate["topics"])
        entity_sim = _jaccard_similarity(
            input_story["location_qids"] | input_story["person_qids"],
            candidate["location_qids"] | candidate["person_qids"],
        )

        combined = 0.6 * emb_sim + 0.2 * topic_sim + 0.2 * entity_sim

        logger.debug(
            "  candidate '%s' -> combined=%.3f (emb=%.3f, topic=%.3f, entity=%.3f)",
            candidate["title"], combined, emb_sim, topic_sim, entity_sim,
        )

        scored.append({
            "story_id": cid,
            "title": candidate["title"],
            "summary": candidate["summary"],
            "similarity_score": combined,
            "embedding_similarity": emb_sim,
            "topic_similarity": topic_sim,
            "entity_similarity": entity_sim,
        })

    scored.sort(key=lambda x: x["similarity_score"], reverse=True)
    top = scored[:n]

    if top:
        logger.info(
            "Top %d similar stories for '%s':", len(top), input_story["title"],
        )
        for s in top:
            logger.info(
                "  [%.3f] '%s' (emb=%.3f, topic=%.3f, entity=%.3f)",
                s["similarity_score"], s["title"],
                s["embedding_similarity"], s["topic_similarity"], s["entity_similarity"],
            )
    else:
        logger.info("No similar stories scored for '%s'", input_story["title"])

    return top


def _load_stories_with_metadata(session, target_date, exclude_story_id):
    """Load stories on target_date with their topics, locations, and persons."""
    from sqlalchemy import cast, Date
    from rds_postgres.models import Story, StoryLocation, StoryPerson, StoryTopic

    query = session.query(Story).filter(
        cast(Story.story_period, Date) == target_date
    )
    if exclude_story_id is not None:
        query = query.filter(Story.id != exclude_story_id)

    stories = query.all()
    story_ids = [s.id for s in stories]
    if not story_ids:
        return []

    topics_by_story = _group_by_story(
        session.query(StoryTopic.story_id, StoryTopic.topic)
        .filter(StoryTopic.story_id.in_(story_ids))
        .all()
    )
    locations_by_story = _group_by_story(
        session.query(StoryLocation.story_id, StoryLocation.wikidata_qid)
        .filter(StoryLocation.story_id.in_(story_ids))
        .all()
    )
    persons_by_story = _group_by_story(
        session.query(StoryPerson.story_id, StoryPerson.wikidata_qid)
        .filter(StoryPerson.story_id.in_(story_ids))
        .all()
    )

    return [
        {
            "story_id": s.id,
            "title": s.title,
            "summary": s.summary,
            "topics": topics_by_story.get(s.id, set()),
            "location_qids": locations_by_story.get(s.id, set()),
            "person_qids": persons_by_story.get(s.id, set()),
        }
        for s in stories
    ]


def _load_single_story_metadata(session, story_id):
    """Load a single story's metadata by its ID."""
    from rds_postgres.models import Story, StoryLocation, StoryPerson, StoryTopic

    story = session.query(Story).filter(Story.id == story_id).first()
    if story is None:
        return None

    topics = {
        row.topic
        for row in session.query(StoryTopic.topic)
        .filter(StoryTopic.story_id == story_id)
        .all()
    }
    location_qids = {
        row.wikidata_qid
        for row in session.query(StoryLocation.wikidata_qid)
        .filter(StoryLocation.story_id == story_id)
        .all()
    }
    person_qids = {
        row.wikidata_qid
        for row in session.query(StoryPerson.wikidata_qid)
        .filter(StoryPerson.story_id == story_id)
        .all()
    }

    return {
        "story_id": story.id,
        "title": story.title,
        "summary": story.summary,
        "topics": topics,
        "location_qids": location_qids,
        "person_qids": person_qids,
    }


def _load_story_embeddings(session, story_ids, embedding_model):
    """Load article embeddings grouped by story ID.

    Returns {story_id: [embedding_vector, ...]}.
    """
    from rds_postgres.models import ArticleEmbedding, ArticleStory

    rows = (
        session.query(ArticleStory.story_id, ArticleEmbedding.embedding)
        .join(ArticleEmbedding, ArticleStory.article_id == ArticleEmbedding.article_id)
        .filter(
            ArticleStory.story_id.in_(story_ids),
            ArticleEmbedding.embedding_model == embedding_model,
        )
        .all()
    )

    result = {}
    for story_id, embedding in rows:
        result.setdefault(story_id, []).append(list(embedding))
    return result


def _group_by_story(rows):
    """Group query rows of (story_id, value) into {story_id: set(values)}."""
    result = {}
    for story_id, value in rows:
        result.setdefault(story_id, set()).add(value)
    return result


def _compute_mean_embedding(embeddings):
    """Average a list of embedding vectors. Returns None if empty."""
    if not embeddings:
        return None
    return np.mean(embeddings, axis=0).tolist()


def _cosine_similarity(a, b):
    """Cosine similarity between two vectors."""
    a = np.array(a)
    b = np.array(b)
    dot = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot / (norm_a * norm_b))


def _jaccard_similarity(set_a, set_b):
    """Jaccard similarity of two sets. Returns 0.0 if both empty."""
    if not set_a and not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)
