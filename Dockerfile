# Stage 1: Build stage
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-base.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements-base.txt

# Stage 2: Final runtime stage
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    "libgeos-c*" \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for runtime security
RUN groupadd --gid 1001 sentinel && \
    useradd --uid 1001 --gid sentinel --shell /bin/false --create-home sentinel

COPY --from=builder /install /usr/local
COPY --chown=sentinel:sentinel . .

USER sentinel