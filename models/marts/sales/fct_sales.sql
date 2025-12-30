{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['order_date'], 'type': 'btree'},
            {'columns': ['source_company'], 'type': 'btree'},
            {'columns': ['customer_key'], 'type': 'btree'}
        ]
    )
}}

with orders as (
    select * from {{ ref('int_all_sales_orders') }}
),

customers as (
    select * from {{ ref('int_all_customers') }}
),

final as (
    select
        -- Keys
        o.order_key,
        o.sale_order_id,
        o.order_number,
        {{ dbt_utils.generate_surrogate_key(['o.source_company', 'o.customer_id']) }} as customer_key,

        -- Dimensions
        o.source_company,
        o.order_status,
        o.invoice_status,

        -- Customer info (denormalized)
        c.partner_name as customer_name,
        c.email as customer_email,
        c.city as customer_city,
        c.is_company as is_business_customer,

        -- Dates
        o.order_date,
        date_trunc('month', o.order_date) as order_month,
        date_trunc('quarter', o.order_date) as order_quarter,
        date_trunc('year', o.order_date) as order_year,
        extract(dow from o.order_date) as day_of_week,
        o.valid_until_date,
        o.commitment_date,

        -- Amounts
        o.amount_untaxed,
        o.amount_tax,
        o.amount_total,
        o.currency_rate,

        -- Metadata
        o.created_at,
        o.updated_at,
        current_timestamp as loaded_at

    from orders o
    left join customers c
        on o.source_company = c.source_company
        and o.customer_id = c.partner_id
)

select * from final
