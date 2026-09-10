# Multi-stage Dockerfile for Enterprise ETL Pipeline & Data Warehouse Synchronizer
# ==============================================================================
# Stage 1: Build & Dependencies
# ==============================================================================
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ==============================================================================
# Stage 2: Minimal Production Runtime
# ==============================================================================
FROM python:3.11-slim AS runtime

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install runtime dependencies for PostgreSQL/SSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Create non-root system user and group for security
RUN groupadd -r etlgroup && useradd -r -g etlgroup -d /app -s /sbin/nologin etluser

# Create necessary staging directories with proper permissions
RUN mkdir -p /app/data_lake_staging /app/logs && chown -R etluser:etlgroup /app

# Copy application source code
COPY --chown=etluser:etlgroup config/ ./config/
COPY --chown=etluser:etlgroup models/ ./models/
COPY --chown=etluser:etlgroup extractors/ ./extractors/
COPY --chown=etluser:etlgroup storage/ ./storage/
COPY --chown=etluser:etlgroup mock_api/ ./mock_api/
COPY --chown=etluser:etlgroup transformers/ ./transformers/
COPY --chown=etluser:etlgroup warehouse/ ./warehouse/
COPY --chown=etluser:etlgroup notifications/ ./notifications/
COPY --chown=etluser:etlgroup dags/ ./dags/
COPY --chown=etluser:etlgroup pipeline/ ./pipeline/
COPY --chown=etluser:etlgroup .env.example .env

# Switch to non-root user
USER etluser

# Healthcheck to verify Python runtime responsiveness
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from config.settings import get_settings; get_settings()" || exit 1

# Default entrypoint runs the full pipeline
ENTRYPOINT ["python", "-m", "pipeline.runner"]
CMD ["--source", "all", "--mode", "mock"]
