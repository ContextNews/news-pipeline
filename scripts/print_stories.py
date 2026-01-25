"""Print stories and their associated articles from the database."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dotenv import load_dotenv


def _format_datetime(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Print stories and save them to JSON.")
    parser.add_argument(
        "--output",
        default="output/stories.json",
        help="Path to write the JSON output",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    load_dotenv()

    from sqlalchemy import text

    from rds_postgres.connection import get_session

    stmt = text(
        """
        SELECT
            s.id AS story_id,
            s.title AS story_title,
            s.generated_at AS generated_at,
            s.updated_at AS updated_at,
            a.id AS article_id,
            a.title AS article_title,
            a.source AS article_source
        FROM stories s
        LEFT JOIN article_stories ast
            ON ast.story_id = s.id
        LEFT JOIN articles a
            ON a.id = ast.article_id
        ORDER BY s.updated_at DESC, s.id, a.published_at DESC, a.id
        """
    )

    with get_session() as session:
        rows = session.execute(stmt).mappings().all()

    if not rows:
        logger.info("No stories found")
        return

    logger.info("Fetched %d story rows", len(rows))
    current_story_id = None
    has_articles = False
    stories: list[dict[str, object]] = []
    current_story: dict[str, object] | None = None
    for row in rows:
        story_id = row["story_id"]
        if story_id != current_story_id:
            if current_story_id is not None:
                if not has_articles:
                    print("- (no articles)")
                print()

            print(f"Story: {story_id} | {row['story_title']}")
            current_story_id = story_id
            has_articles = False
            current_story = {
                "id": story_id,
                "title": row["story_title"],
                "generated_at": _format_datetime(row["generated_at"]),
                "updated_at": _format_datetime(row["updated_at"]),
                "articles": [],
            }
            stories.append(current_story)

        article_id = row["article_id"]
        article_title = row["article_title"]
        if article_id is None and article_title is None:
            continue

        print(f"- {article_id} | {article_title}")
        has_articles = True
        if current_story is not None:
            current_story["articles"].append(
                {
                    "id": article_id,
                    "title": article_title,
                    "source": row["article_source"],
                }
            )

    if current_story_id is not None and not has_articles:
        print("- (no articles)")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(stories, handle, indent=2)
    logger.info("Wrote %d stories to %s", len(stories), output_path)


if __name__ == "__main__":
    main()
