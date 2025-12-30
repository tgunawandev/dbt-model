# CLAUDE.md - AI Assistant Instructions

This file provides context for AI assistants (like Claude) working with this dbt project.

## Project Overview

This is a **dbt (data build tool)** project that builds a data warehouse for multi-company analytics. It consolidates data from three Odoo ERP databases (TLN, TMI, IEG) into a unified analytics layer.

## Key Technical Details

### Database Architecture

- **Target Database**: `analytics_db` on PostgreSQL server `116.203.191.172:5432`
- **Source Databases**: Connected via PostgreSQL Foreign Data Wrapper (FDW)
  - `tln_db` -> imported into `tln_raw` schema
  - `tmi_db` -> imported into `tmi_raw` schema
  - `ieg_db` -> imported into `ieg_raw` schema

### Why FDW?

dbt-postgres can only connect to one database at a time. FDW allows us to access multiple databases through a single connection by importing foreign tables into the target database.

### Profile Configuration

The dbt profile is located at `~/.dbt/profiles.yml` (not in the repo for security). The project uses environment variables for credentials:

```yaml
dbt_model:
  target: dev
  outputs:
    dev:
      type: postgres
      host: 116.203.191.172
      port: 5432
      user: postgres
      dbname: analytics_db
```

## Common Tasks

### Running dbt

```bash
# With Docker
docker compose run --rm dbt run

# Without Docker (requires local dbt installation)
dbt run
```

### Adding a New Source Table

1. Add table to the source YAML file (e.g., `_tln_sources.yml`)
2. Import the foreign table if needed:
   ```sql
   IMPORT FOREIGN SCHEMA public
       LIMIT TO (new_table_name)
       FROM SERVER tln_server INTO tln_raw;
   ```
3. Create staging model

### Adding a New Company/Database

1. Create FDW server and user mapping in `analytics_db`
2. Create foreign schema and import tables
3. Add source YAML file in `models/staging/<company>_db/`
4. Create staging models
5. Update intermediate models to union new company data
6. Update `dbt_project.yml` with new company code variable

### Creating a New Model

Follow the naming conventions:
- Staging: `stg_<source>__<entity>.sql` (e.g., `stg_tln__sales_orders.sql`)
- Intermediate: `int_<description>.sql` (e.g., `int_all_sales_orders.sql`)
- Marts: `fct_<entity>.sql` or `dim_<entity>.sql`

## Code Style Guidelines

### SQL

- Use lowercase for SQL keywords
- Use CTEs (Common Table Expressions) for readability
- Prefix CTEs with descriptive names (`source`, `renamed`, `joined`, `final`)
- Use explicit column names, avoid `SELECT *` in final output
- Add `source_company` column to all staging models

### YAML

- Always include descriptions for sources and models
- Add tests for primary keys (`unique`, `not_null`)
- Use `accepted_values` tests for enum columns

### Jinja

- Use `{{ ref() }}` for model references
- Use `{{ source() }}` for source references
- Use `{{ var() }}` for configuration variables

## Testing

```bash
# Run all tests
dbt test

# Test specific model
dbt test --select fct_sales

# Run with verbose output
dbt test --debug
```

### Test Types

1. **Schema tests** - Defined in YAML files (unique, not_null, accepted_values)
2. **Data tests** - Custom SQL in `tests/` directory
3. **Source freshness** - Check source data recency

## Materialization Strategy

| Layer | Materialization | Reason |
|-------|-----------------|--------|
| Staging | view | Minimal storage, always current |
| Intermediate | ephemeral | No physical table, compiles into downstream |
| Marts | table | Performance for analytics queries |

## Important Files

| File | Purpose |
|------|---------|
| `dbt_project.yml` | Project configuration, model settings |
| `packages.yml` | External dbt packages (dbt_utils, codegen) |
| `profiles.yml.example` | Template for database connection |
| `Dockerfile` | Docker image for running dbt |
| `docker-compose.yml` | Docker services configuration |
| `.github/workflows/ci.yml` | CI pipeline configuration |
| `.github/workflows/docker-build.yml` | Docker build pipeline |

## Troubleshooting

### Connection Issues

```bash
# Test connection
dbt debug

# Check FDW status
psql analytics_db -c "SELECT * FROM pg_foreign_server;"
```

### Model Compilation Errors

```bash
# Compile without running
dbt compile --select model_name

# Check compiled SQL
cat target/compiled/dbt_model/models/.../model_name.sql
```

### Missing Source Tables

If a source table is missing, import it via FDW:

```sql
IMPORT FOREIGN SCHEMA public
    LIMIT TO (table_name)
    FROM SERVER <company>_server INTO <company>_raw;
```

## Data Lineage

```
Source (Odoo DBs)
    ↓
Foreign Tables (FDW)
    ↓
Staging Models (stg_*)
    ↓
Intermediate Models (int_*)
    ↓
Mart Models (fct_*, dim_*)
    ↓
BI Tools / Analytics
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DBT_POSTGRES_HOST` | Database host | 116.203.191.172 |
| `DBT_POSTGRES_PORT` | Database port | 5432 |
| `DBT_POSTGRES_USER` | Database user | postgres |
| `DBT_POSTGRES_PASSWORD` | Database password | (required) |
| `DBT_TARGET` | dbt target (dev/staging/prod) | dev |
