FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

# Dependencies first, cached separately from app code so a src/ change
# doesn't invalidate this layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY src/ src/
COPY alembic/ alembic/
COPY alembic.ini README.md ./
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
