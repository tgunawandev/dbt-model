FROM python:3.11-slim

LABEL maintainer="tgunawandev"
LABEL description="dbt Data Warehouse for Multi-Company Odoo Analytics"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/dbt

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /dbt

# Install dbt-postgres
RUN pip install --no-cache-dir \
    dbt-postgres==1.7.9 \
    sqlfluff==3.0.7 \
    sqlfluff-templater-dbt==3.0.7

# Copy project files
COPY dbt_project.yml packages.yml ./
COPY models/ ./models/
COPY macros/ ./macros/
COPY seeds/ ./seeds/
COPY snapshots/ ./snapshots/
COPY analyses/ ./analyses/
COPY tests/ ./tests/

# Install dbt packages
RUN dbt deps --profiles-dir /dbt || true

# Default command
ENTRYPOINT ["dbt"]
CMD ["--help"]
