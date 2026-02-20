"""CLI for classifying articles by topic."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from rds_postgres.connection import get_session

from classify_articles.classify_articles import classify_articles
from classify_articles.helpers import parse_classify_articles_args
from common.aws import load_articles_for_classification, upload_article_topics, upload_jsonl_records_to_s3
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_classify_articles_args()

    articles = load_articles_for_classification(args.published_date, args.overwrite)
    if not articles:
        logger.warning("No articles to classify")
        return

    results = classify_articles(
        articles=articles,
        model=args.model,
        batch_size=args.batch_size,
        threshold=args.threshold,
        word_limit=args.word_limit,
    )

    if not results:
        logger.warning("No articles classified")
        return

    titles_by_id = {a["id"]: a.get("title", "untitled") for a in articles}
    for result in results:
        logger.info(
            "  %s | %s | topics=%s",
            result.article_id,
            titles_by_id.get(result.article_id, "untitled"),
            result.topics,
        )

    if args.load_s3:
        upload_jsonl_records_to_s3(results, "classified_articles")

    if args.load_local:
        save_jsonl_records_local(results, "classified_articles")

    if args.load_rds:
        with get_session() as session:
            upload_article_topics(results, session, args.overwrite)


if __name__ == "__main__":
    main()
