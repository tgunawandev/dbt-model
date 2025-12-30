# dbt-model

A dbt (data build tool) project for building a data warehouse from multiple Odoo ERP databases. This project consolidates data from TLN, TMI, and IEG companies into a unified analytics layer with **accounting as the source of truth**.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      OPERATIONAL MODULES                            │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌──────────────────┐    │
│  │  Sales  │  │ Purchase │  │ Inventory │  │  Manufacturing   │    │
│  └────┬────┘  └────┬─────┘  └─────┬─────┘  └────────┬─────────┘    │
│       └────────────┴──────────────┴──────────────────┘              │
│                              │                                      │
│                              ▼                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │               ACCOUNTING (Source of Truth)                    │  │
│  │  account_move → account_move_line (General Ledger)           │  │
│  │  All financial metrics derive from here                       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Finance Mart = Source of Truth** - All financial metrics (revenue, expenses, P&L) come from the General Ledger
2. **Operational Marts = Context** - Sales, inventory details for operational analytics
3. **Bridge Tables = Reconciliation** - Link operational data to GL for drill-through

## Project Structure

```
dbt-model/
├── models/
│   ├── staging/                    # Organized by module (not by company)
│   │   ├── accounting/             # Core financial data
│   │   │   ├── stg_account_move.sql
│   │   │   ├── stg_account_move_line.sql  # 6.6M+ records
│   │   │   └── stg_account_account.sql
│   │   ├── sales/
│   │   │   ├── stg_sale_order.sql
│   │   │   └── stg_sale_order_line.sql
│   │   ├── inventory/
│   │   │   └── stg_stock_move.sql
│   │   ├── purchasing/
│   │   └── master/                 # Shared dimensions
│   │       ├── stg_res_partner.sql
│   │       └── stg_product_product.sql
│   └── marts/
│       ├── finance/                # PRIMARY - Financial reporting
│       │   ├── dim_account.sql     # Chart of Accounts
│       │   ├── fct_general_ledger.sql  # SOURCE OF TRUTH
│       │   └── fct_trial_balance.sql
│       ├── sales/                  # Operational
│       │   ├── fct_sales_orders.sql
│       │   ├── dim_customers.sql
│       │   └── bridge_sales_to_gl.sql  # Reconciliation
│       └── inventory/
│           ├── fct_stock_movements.sql
│           └── bridge_stock_to_gl.sql  # Reconciliation
├── tests/
│   └── reconciliation/             # Validate operational = GL
│       ├── sales_vs_revenue.sql
│       └── inventory_vs_cogs.sql
├── Dockerfile
├── docker-compose.yml
└── .github/workflows/
```

## Multi-Company Data Model

Each staging model unions data from all three companies:

```sql
-- Example: All companies in one model
WITH tln AS (SELECT *, 'tln' AS source_company FROM {{ source('tln_db', 'account_move') }}),
     tmi AS (SELECT *, 'tmi' AS source_company FROM {{ source('tmi_db', 'account_move') }}),
     ieg AS (SELECT *, 'ieg' AS source_company FROM {{ source('ieg_db', 'account_move') }})
SELECT * FROM tln UNION ALL SELECT * FROM tmi UNION ALL SELECT * FROM ieg
```

## Quick Start

```bash
# Using Docker
docker compose run --rm dbt deps     # Install packages
docker compose run --rm dbt-run      # Run all models
docker compose up dbt-docs           # Docs at localhost:8080

# Local (requires dbt-postgres)
dbt deps && dbt run && dbt test
```

## When to Use What

| Question Type | Use This Mart | Why |
|--------------|---------------|-----|
| Revenue, P&L, Balance Sheet | `finance/*` | **Accounting is truth** |
| Sales pipeline, order details | `sales/*` | Operational context |
| Inventory levels, movements | `inventory/*` | Physical tracking |
| "Why doesn't X match Y?" | `bridge_*` | Reconciliation |

## Key Models

### fct_general_ledger (Source of Truth)
The core fact table from `account_move_line`. All financial reports derive from this.

### bridge_sales_to_gl
Links sales orders to their corresponding GL entries for reconciliation:
```sql
SELECT order_number, account_code, debit, credit
FROM bridge_sales_to_gl
WHERE order_number = 'SO12345'
```

## Database Setup

Uses PostgreSQL Foreign Data Wrapper (FDW) to access multiple databases:
- `tln_raw` schema → tln_db
- `tmi_raw` schema → tmi_db
- `ieg_raw` schema → ieg_db

All queries run against `analytics_db` which accesses source databases via FDW.

## CI/CD

- **ci.yml**: Lint and compile on PRs
- **docker-build.yml**: Build and push to GHCR on main

## License

Proprietary - Internal Use Only
