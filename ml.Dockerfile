FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

# Install base requirements first (Docker will cache this layer from the standard Dockerfile!)
COPY requirements-base.txt .
RUN pip install --no-cache-dir -r requirements-base.txt

# Install Heavy ML requirements
COPY requirements-ml.txt .
RUN pip install --no-cache-dir -r requirements-ml.txt
RUN pip install --no-cache-dir https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.7.1/en_core_web_sm-3.7.1.tar.gz

COPY . .