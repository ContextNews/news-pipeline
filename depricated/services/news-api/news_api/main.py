"""FastAPI application entry point."""

import os

from fastapi import FastAPI

from news_api.config import load_config, set_config
from news_api.routers import articles, health, raw_articles, stories

# Load config on startup
config_name = os.getenv("NEWS_API_CONFIG", "prod")
config = load_config(config_name)
set_config(config)

app = FastAPI(
    title="News API",
    description="REST API for accessing normalized news articles and clustered stories",
    version="1.0.0",
)

# Register routers
app.include_router(health.router)
app.include_router(articles.router)
app.include_router(raw_articles.router)
app.include_router(stories.router)


@app.get("/")
async def root():
    """API root - returns basic info."""
    return {
        "name": "News API",
        "version": "1.0.0",
        "docs": "/docs",
    }


def main():
    """Run the API server."""
    import uvicorn

    uvicorn.run(
        "news_api.main:app",
        host=config.server.host,
        port=config.server.port,
        reload=True,
    )


if __name__ == "__main__":
    main()
