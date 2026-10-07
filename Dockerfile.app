# Start from the minimal official Python slim image
FROM python:3.12-slim

# get uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set up your working directory
WORKDIR /app


# Copy dependency configuration files first to leverage Docker layer caching
COPY pyproject.toml uv.lock ./
COPY ecom_pipeline ./ecom_pipeline
COPY data ./data

# Run uv sync to install dependencies without the project source code yet
RUN uv sync --frozen --no-dev



CMD ["uv", "run"]