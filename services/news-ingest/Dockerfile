FROM python:3.12-slim

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies (no dev deps, no virtualenv in container)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main

# Copy application code
COPY news_ingest/ news_ingest/
COPY configs/ configs/

ENTRYPOINT ["python", "-m", "news_ingest.cli"]
