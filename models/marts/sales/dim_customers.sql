{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['customer_key'], 'type': 'btree'},
            {'columns': ['source_company'], 'type': 'btree'}
        ]
    )
}}

-- Customer Dimension
-- Combines partner data with order metrics

with customers as (
    select * from {{ ref('stg_res_partner') }}
    where customer_rank > 0  -- Only customers
),

customer_orders as (
    select
        customer_key,
        count(*) as total_orders,
        sum(amount_total) as total_revenue,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        avg(amount_total) as avg_order_value
    from {{ ref('fct_sales_orders') }}
    where order_state in ('sale', 'done')
    group by 1
),

-- Revenue from GL (for reconciliation)
customer_gl_revenue as (
    select
        partner_key,
        sum(credit - debit) as gl_revenue
    from {{ ref('fct_general_ledger') }}
    where financial_statement_section = 'Revenue'
      and partner_id is not null
    group by 1
),

final as (
    select
        -- Keys
        c.partner_key as customer_key,
        c.partner_id,
        c.source_company,

        -- Customer info
        c.partner_name,
        c.display_name,
        c.email,
        c.phone,
        c.mobile,
        c.website,

        -- Address
        c.street,
        c.street2,
        c.city,
        c.postal_code,
        c.country_id,
        c.state_id,

        -- Business info
        c.tax_id,
        c.is_company,
        c.customer_rank,
        c.supplier_rank,

        -- Order metrics (from sales module)
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.total_revenue, 0) as total_sales_revenue,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.avg_order_value, 0) as avg_order_value,

        -- Revenue from GL (for reconciliation)
        coalesce(gl.gl_revenue, 0) as total_gl_revenue,

        -- Customer segmentation
        case
            when coalesce(co.total_revenue, 0) >= 100000 then 'Enterprise'
            when coalesce(co.total_revenue, 0) >= 50000 then 'Mid-Market'
            when coalesce(co.total_revenue, 0) >= 10000 then 'SMB'
            when coalesce(co.total_revenue, 0) > 0 then 'Small'
            else 'Prospect'
        end as customer_segment,

        -- Status
        c.is_active,
        c.created_at,
        c.updated_at,
        current_timestamp as loaded_at

    from customers c
    left join customer_orders co on c.partner_key = co.customer_key
    left join customer_gl_revenue gl on c.partner_key = gl.partner_key
)

select * from final
