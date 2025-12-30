{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['gl_line_key'], 'type': 'btree'},
            {'columns': ['posting_date'], 'type': 'btree'},
            {'columns': ['account_key'], 'type': 'btree'},
            {'columns': ['source_company'], 'type': 'btree'},
            {'columns': ['move_type'], 'type': 'btree'}
        ]
    )
}}

-- General Ledger Fact Table
-- THE SOURCE OF TRUTH for all financial reporting
-- All financial metrics should derive from this table

with gl_lines as (
    select * from {{ ref('stg_account_move_line') }}
),

moves as (
    select * from {{ ref('stg_account_move') }}
),

accounts as (
    select * from {{ ref('dim_account') }}
),

final as (
    select
        -- Keys
        gl.gl_line_key,
        gl.gl_line_id,
        {{ dbt_utils.generate_surrogate_key(['gl.source_company', 'gl.move_id']) }} as move_key,
        {{ dbt_utils.generate_surrogate_key(['gl.source_company', 'gl.account_id']) }} as account_key,
        {{ dbt_utils.generate_surrogate_key(['gl.source_company', 'gl.partner_id']) }} as partner_key,
        {{ dbt_utils.generate_surrogate_key(['gl.source_company', 'gl.product_id']) }} as product_key,

        -- Company
        gl.source_company,
        gl.company_id,

        -- Account info (denormalized for performance)
        gl.account_id,
        a.account_code,
        a.account_name,
        a.account_type,
        a.financial_statement_section,
        a.statement_type,
        a.normal_balance,

        -- Move info
        gl.move_id,
        m.move_name,
        m.move_type,
        m.reference,
        m.invoice_origin,  -- Links to sales orders!

        -- Relationships
        gl.partner_id,
        gl.product_id,
        gl.journal_id,
        gl.analytic_account_id,

        -- THE CORE FINANCIAL DATA
        gl.debit,
        gl.credit,
        gl.balance,

        -- Multi-currency
        gl.amount_currency,
        gl.currency_id,

        -- For invoice/bill lines
        gl.quantity,
        gl.price_unit,
        gl.discount,
        gl.price_subtotal,
        gl.price_total,

        -- Tax
        gl.tax_line_id,
        gl.tax_base_amount,

        -- Dates
        gl.posting_date,
        m.invoice_date,
        m.due_date,
        date_trunc('month', gl.posting_date) as posting_month,
        date_trunc('quarter', gl.posting_date) as posting_quarter,
        date_trunc('year', gl.posting_date) as posting_year,
        extract(year from gl.posting_date) as fiscal_year,

        -- Reconciliation status
        gl.is_reconciled,
        gl.full_reconcile_id,
        gl.amount_residual,
        gl.matching_number,

        -- Operational links (for drill-through to modules)
        gl.stock_move_id,      -- Link to inventory movements
        gl.purchase_line_id,   -- Link to purchase orders

        -- Line info
        gl.line_description,
        gl.display_type,
        gl.is_blocked,

        -- Metadata
        gl.created_at,
        current_timestamp as loaded_at

    from gl_lines gl
    inner join moves m on gl.source_company = m.source_company and gl.move_id = m.move_id
    left join accounts a on gl.source_company = a.source_company and gl.account_id = a.account_id
)

select * from final
