{{
    config(
        materialized='view'
    )
}}

-- Union general ledger lines from all companies (6.6M+ records)
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'account_move_line') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'account_move_line') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'account_move_line') }}
),

unioned as (
    select * from tln
    union all
    select * from tmi
    union all
    select * from ieg
),

renamed as (
    select
        -- Surrogate key (globally unique)
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as gl_line_key,

        -- Natural keys
        id as gl_line_id,
        source_company,
        move_id,
        move_name,

        -- Account info
        account_id,
        account_internal_type,

        -- Dates
        date as posting_date,
        date_maturity,
        create_date as created_at,

        -- Description
        name as line_description,
        ref as reference,

        -- Amounts - THE CORE LEDGER DATA
        debit,
        credit,
        balance,  -- debit - credit

        -- Multi-currency
        amount_currency,
        currency_id,

        -- Pricing (for invoice lines)
        quantity,
        price_unit,
        discount,
        price_subtotal,
        price_total,

        -- Tax
        tax_line_id,
        tax_base_amount,

        -- Relationships
        partner_id,
        product_id,
        product_uom_id,
        journal_id,
        company_id,
        analytic_account_id,

        -- Reconciliation
        reconciled as is_reconciled,
        full_reconcile_id,
        amount_residual,
        amount_residual_currency,
        matching_number,

        -- Operational links (for drill-through to modules)
        stock_move_id,       -- Link to inventory
        purchase_line_id,    -- Link to purchasing

        -- Status
        parent_state as move_state,
        display_type,
        blocked as is_blocked

    from unioned
    where parent_state = 'posted'  -- Only lines from posted entries
)

select * from renamed
