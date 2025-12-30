# CLAUDE.md - AI Assistant Instructions

## Project Overview

This is a **dbt data warehouse** for multi-company Odoo ERP analytics. The key design principle is:

**Accounting (General Ledger) = Source of Financial Truth**

All financial metrics MUST derive from `fct_general_ledger`. Operational modules (sales, inventory) provide context and detail.

## Architecture

```
Staging Layer (by module, not company)
├── accounting/  → stg_account_move, stg_account_move_line (6.6M rows)
├── sales/       → stg_sale_order, stg_sale_order_line
├── inventory/   → stg_stock_move
├── purchasing/  → stg_purchase_order
└── master/      → stg_res_partner, stg_product_product

Mart Layer
├── finance/     → fct_general_ledger (SOURCE OF TRUTH)
├── sales/       → fct_sales_orders + bridge_sales_to_gl
└── inventory/   → fct_stock_movements + bridge_stock_to_gl
```

## Key Design Decisions

1. **Module-based staging** - Organized by Odoo module, not company
2. **Union pattern** - Each staging model unions tln + tmi + ieg with `source_company` column
3. **Surrogate keys** - Use `dbt_utils.generate_surrogate_key(['source_company', 'id'])`
4. **Bridge tables** - Link operational facts to GL for reconciliation

## Database Connection

- **Target**: `analytics_db` on PostgreSQL 116.203.191.172:5432
- **Source access**: Via Foreign Data Wrapper (FDW)
  - `tln_raw.*` → tln_db tables
  - `tmi_raw.*` → tmi_db tables
  - `ieg_raw.*` → ieg_db tables

## Common Tasks

### Add a new Odoo table
1. Add to source YAML (e.g., `_accounting_sources.yml`)
2. Create staging model with union pattern
3. Link to appropriate mart

### Check financial reconciliation
```sql
-- Sales vs Revenue
SELECT source_company,
       SUM(amount_total) as sales_total,
       (SELECT SUM(credit-debit) FROM fct_general_ledger
        WHERE financial_statement_section = 'Revenue') as gl_revenue
FROM fct_sales_orders GROUP BY 1
```

### Drill from sales order to GL entries
```sql
SELECT * FROM bridge_sales_to_gl WHERE order_number = 'SO12345'
```

## Model Naming

- Staging: `stg_<table>.sql` (e.g., `stg_account_move.sql`)
- Facts: `fct_<entity>.sql` (e.g., `fct_general_ledger.sql`)
- Dimensions: `dim_<entity>.sql` (e.g., `dim_account.sql`)
- Bridges: `bridge_<from>_to_<to>.sql` (e.g., `bridge_sales_to_gl.sql`)

## Key Models

| Model | Purpose |
|-------|---------|
| `fct_general_ledger` | **SOURCE OF TRUTH** for all financial reporting |
| `dim_account` | Chart of Accounts with financial statement classification |
| `fct_trial_balance` | Monthly aggregated by account |
| `bridge_sales_to_gl` | Sales → Invoice → GL entries |
| `bridge_stock_to_gl` | Stock movements → GL entries |

## Testing

```bash
dbt test --select tag:core   # Test core financial models
dbt test --select reconciliation  # Run reconciliation tests
```

## Important Files

- `dbt_project.yml` - Project config, model settings
- `profiles.yml.example` - Connection template
- `tests/reconciliation/` - Sales/Inventory vs GL checks
