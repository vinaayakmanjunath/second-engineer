# Pipeline + API image. The API reads the Postgres `serving` schema that `secondeng publish` loads
# from gold. Runtime env: DATABASE_URL (read-only role), API_KEYS (JSON key -> owner), PORT (Render).
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
# exec uvicorn directly: honours $PORT and skips the pipeline's heavy imports at startup
CMD ["sh", "-c", "exec uvicorn secondeng.api:app --host 0.0.0.0 --port ${PORT:-8000}"]
