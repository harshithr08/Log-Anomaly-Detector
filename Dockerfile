# One image for every Python process; docker-compose picks the command.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 1. Dependencies only (cached until pyproject.toml changes).
COPY pyproject.toml README.md ./
RUN mkdir -p src/logad src/shop \
    && touch src/logad/__init__.py src/shop/__init__.py \
    && pip install . \
    && rm -rf src build

# 2. Our code (rebuilds in seconds).
COPY src ./src
RUN pip install --no-deps .
COPY alembic.ini ./
COPY migrations ./migrations
