{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['sale_order_key'], 'type': 'btree'},
            {'columns': ['order_date'], 'type': 'btree'},
            {'columns': ['source_company'], 'type': 'btree'},
            {'columns': ['customer_key'], 'type': 'btree'}
        ]
    )
}}

-- Sales Orders Fact Table
-- Operational metrics for sales analytics
-- Use bridge_sales_to_gl to reconcile with financial data

with orders as (
    select * from {{ ref('stg_sale_order') }}
),

customers as (
    select * from {{ ref('stg_res_partner') }}
),

-- Link to invoices via invoice_origin
invoices as (
    select
        source_company,
        invoice_origin,
        move_id as invoice_id,
        move_name as invoice_number,
        invoice_date,
        amount_total as invoiced_amount,
        payment_state,
        amount_due
    from {{ ref('stg_account_move') }}
    where move_type = 'out_invoice'
),

final as (
    select
        -- Keys
        o.sale_order_key,
        o.sale_order_id,
        o.source_company,
        o.order_number,
        {{ dbt_utils.generate_surrogate_key(['o.source_company', 'o.customer_id']) }} as customer_key,

        -- Customer info (denormalized)
        o.customer_id,
        c.partner_name as customer_name,
        c.email as customer_email,
        c.city as customer_city,
        c.is_company as is_business_customer,

        -- Status
        o.order_state,
        o.invoice_status,

        -- Dates
        o.order_date,
        date_trunc('month', o.order_date) as order_month,
        date_trunc('quarter', o.order_date) as order_quarter,
        date_trunc('year', o.order_date) as order_year,
        o.validity_date,
        o.commitment_date,

        -- Amounts from Sales Order
        o.amount_untaxed,
        o.amount_tax,
        o.amount_total,

        -- Sales team
        o.salesperson_id,
        o.sales_team_id,

        -- Link to accounting
        i.invoice_id,
        i.invoice_number,
        i.invoice_date,
        i.invoiced_amount,
        i.payment_state,
        i.amount_due as invoice_amount_due,

        -- Reconciliation check
        coalesce(i.invoiced_amount, 0) as gl_invoiced_amount,
        o.amount_total - coalesce(i.invoiced_amount, 0) as invoice_variance,

        -- Metadata
        o.created_at,
        o.updated_at,
        current_timestamp as loaded_at

    from orders o
    left join customers c
        on o.source_company = c.source_company
        and o.customer_id = c.partner_id
    left join invoices i
        on o.source_company = i.source_company
        and o.order_number = i.invoice_origin
)

select * from final
