import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from news_normalize.extract.schema import NormalizedArticle
from news_normalize.io.s3 import is_s3_path, write_s3_bytes


def articles_to_table(articles: list[NormalizedArticle]) -> pa.Table:
    """Convert articles to a PyArrow table."""
    rows = [article.to_dict() for article in articles]
    return pa.Table.from_pylist(rows)


def write_parquet(articles: list[NormalizedArticle], path: str) -> None:
    """Write articles to a Parquet file (local or S3)."""
    table = articles_to_table(articles)

    if is_s3_path(path):
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        write_s3_bytes(path, buffer.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
