FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock* ./
RUN uv sync --all-extras --no-install-project

COPY pluggy2duckdb/ ./pluggy2duckdb/
COPY config/ ./config/
COPY tests/ ./tests/
RUN uv sync --all-extras

ENTRYPOINT ["uv", "run"]
CMD ["pluggy-raw"]
