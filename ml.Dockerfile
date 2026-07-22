# Stage 1: Builder stage for base & ML dependencies
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

COPY requirements-ml.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements-ml.txt --extra-index-url https://download.pytorch.org/whl/cpu
RUN pip install --prefix=/install --no-cache-dir https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.7.1/en_core_web_sm-3.7.1.tar.gz

# Stage 2: Final runtime stage
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    KMP_DUPLICATE_LIB_OK=TRUE \
    OMP_NUM_THREADS=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    "libgeos-c*" \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY . .