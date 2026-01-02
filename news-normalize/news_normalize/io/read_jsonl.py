import gzip
import json
from typing import Iterator

from news_normalize.io.s3 import is_s3_path, read_s3_bytes


def read_jsonl(path: str) -> Iterator[dict]:
    """Stream JSON objects from a JSONL file (local or S3, gzip or plain)."""
    if is_s3_path(path):
        content = read_s3_bytes(path)
        if path.endswith(".gz"):
            content = gzip.decompress(content)
        for line in content.decode("utf-8").splitlines():
            line = line.strip()
            if line:
                yield json.loads(line)
    else:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
