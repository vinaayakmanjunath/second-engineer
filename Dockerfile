# Pipeline + API image. The API reads a DuckDB warehouse built by `secondeng run`;
# mount it and point SECONDENG_WAREHOUSE at it. API keys come from the API_KEYS env var.
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv PATH="/opt/venv/bin:$PATH"
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY config ./config
COPY dbt ./dbt
RUN uv sync --frozen --no-dev
RUN useradd --create-home app && chown -R app /app
USER app
EXPOSE 8000
CMD ["secondeng", "api", "--port", "8000"]
