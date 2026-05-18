"""CLI for extracting named entities from articles."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from context_db.connection import get_session

from extract_entities.extract_entities import extract_entities
from extract_entities.helpers import parse_extract_entities_args
from common.db_io import load_articles_for_entities, upload_entities
from common.object_storage import upload_jsonl_records_to_object_store
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:

    # Parse CLI arguments
    args = parse_extract_entities_args()
    articles = load_articles_for_entities(args.published_date, args.overwrite)

    if not articles:
        logger.warning("No articles to process")
        return

    # Extract entities
    entities = extract_entities(
        articles=articles,
        model=args.model,
        batch_size=args.batch_size,
        word_limit=args.word_limit,
    )

    if not entities:
        logger.warning("No entities extracted")
        return

    if args.load_object_store:
        upload_jsonl_records_to_object_store(entities, "article_entity_mentions")

    if args.load_local:
        save_jsonl_records_local(entities, "article_entity_mentions")

    if args.load_db:
        with get_session() as session:
            upload_entities(entities, session, args.overwrite)


if __name__ == "__main__":
    main()
