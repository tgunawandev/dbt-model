# dbt-model

A dbt (data build tool) project for building a data warehouse from multiple Odoo ERP databases. This project consolidates data from TLN, TMI, and IEG companies into a unified analytics layer.

## Architecture

```
                    +----------------+
                    |  analytics_db  |
                    |   (Target DW)  |
                    +----------------+
                           |
         +--------+--------+--------+
         |        |                 |
    +----v----+  +----v----+  +----v----+
    | tln_raw |  | tmi_raw |  | ieg_raw |
    |  (FDW)  |  |  (FDW)  |  |  (FDW)  |
    +---------+  +---------+  +---------+
         |            |            |
    +----v----+  +----v----+  +----v----+
    | tln_db  |  | tmi_db  |  | ieg_db  |
    |  Odoo   |  |  Odoo   |  |  Odoo   |
    +---------+  +---------+  +---------+
```

### Multi-Database Strategy

dbt-postgres connects to one database at a time. To work with multiple source databases, we use **PostgreSQL Foreign Data Wrapper (FDW)**:

1. All Odoo databases are accessed via FDW from the `analytics_db` target database
2. Foreign schemas (`tln_raw`, `tmi_raw`, `ieg_raw`) contain imported tables
3. dbt models reference these foreign tables as sources
4. Transformed data is materialized in `staging`, `intermediate`, and `marts` schemas

## Prerequisites

- Docker and Docker Compose
- PostgreSQL client (optional, for debugging)
- GitHub CLI (for repository operations)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/tgunawandev/dbt-model.git
cd dbt-model
```

### 2. Set Up Environment Variables

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Run dbt with Docker

```bash
# Build the Docker image
docker compose build

# Run dbt debug to test connection
docker compose run --rm dbt debug

# Install dbt packages
docker compose run --rm dbt deps

# Run all models
docker compose run --rm dbt-run

# Run tests
docker compose run --rm dbt-test

# Generate and serve documentation
docker compose up dbt-docs
# Visit http://localhost:8080
```

### 4. Local Development (without Docker)

```bash
# Install dbt-postgres
pip install dbt-postgres==1.7.9

# Copy profile to dbt config directory
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your credentials

# Install dependencies
dbt deps

# Test connection
dbt debug

# Run models
dbt run

# Run tests
dbt test
```

## Project Structure

```
dbt-model/
├── dbt_project.yml          # Project configuration
├── packages.yml             # dbt package dependencies
├── profiles.yml.example     # Profile template
├── Dockerfile               # Docker image definition
├── docker-compose.yml       # Docker services
├── .github/
│   └── workflows/
│       ├── ci.yml           # CI pipeline
│       └── docker-build.yml # Docker build pipeline
├── models/
│   ├── staging/             # Source data staging
│   │   ├── tln_db/          # TLN company models
│   │   ├── tmi_db/          # TMI company models
│   │   └── ieg_db/          # IEG company models
│   ├── intermediate/        # Business logic
│   └── marts/               # Analytics tables
│       ├── sales/           # Sales analytics
│       ├── inventory/       # Inventory analytics
│       └── finance/         # Financial analytics
├── macros/                  # Custom Jinja macros
├── seeds/                   # Static CSV data
├── snapshots/               # SCD Type 2 tables
├── analyses/                # Ad-hoc analyses
└── tests/                   # Custom data tests
```

## Data Models

### Staging Layer (`models/staging/`)

Raw data from Odoo with minimal transformations:
- Column renaming for consistency
- Type casting
- Filtering inactive/cancelled records
- Adding `source_company` identifier

### Intermediate Layer (`models/intermediate/`)

Business logic transformations:
- `int_all_sales_orders` - Unified sales orders from all companies
- `int_all_customers` - Unified customer data

### Marts Layer (`models/marts/`)

Analytics-ready tables:
- `fct_sales` - Fact table with all sales transactions
- `dim_customers` - Customer dimension with aggregated metrics
- `sales_summary` - Daily sales metrics with period comparisons

## Common Commands

```bash
# Run specific models
dbt run --select staging.tln_db     # Run TLN staging models
dbt run --select marts.sales        # Run sales mart models
dbt run --select +fct_sales         # Run fct_sales and all upstream

# Run with full refresh (for incremental models)
dbt run --full-refresh

# Test specific models
dbt test --select fct_sales

# Generate documentation
dbt docs generate
dbt docs serve

# Compile SQL without running
dbt compile

# List all models
dbt ls
```

## CI/CD Pipeline

### Continuous Integration (ci.yml)

Triggered on push/PR to `main` or `develop`:
1. **SQL Linting** - SQLFluff checks
2. **dbt Compile** - Validates SQL syntax
3. **dbt Test** - Runs data tests (on PRs)

### Docker Build (docker-build.yml)

Triggered on push to `main` or version tags:
1. Builds Docker image
2. Pushes to GitHub Container Registry (GHCR)
3. Tags with commit SHA and version

### Required GitHub Secrets

- `DBT_POSTGRES_HOST` - Database host
- `DBT_POSTGRES_PORT` - Database port
- `DBT_POSTGRES_USER` - Database user
- `DBT_POSTGRES_PASSWORD` - Database password

## Database Setup

### Foreign Data Wrapper Configuration

The FDW is pre-configured in `analytics_db`:

```sql
-- Foreign servers
tln_server -> tln_db
tmi_server -> tmi_db
ieg_server -> ieg_db

-- Foreign schemas (imported tables)
tln_raw.* -> tln_db.public.*
tmi_raw.* -> tmi_db.public.*
ieg_raw.* -> ieg_db.public.*

-- dbt target schemas
staging   -> Staging models
intermediate -> Intermediate models
marts     -> Mart models
```

## Contributing

1. Create a feature branch from `develop`
2. Make your changes
3. Run `dbt compile` and `dbt test`
4. Submit a pull request

## License

Proprietary - Internal Use Only
