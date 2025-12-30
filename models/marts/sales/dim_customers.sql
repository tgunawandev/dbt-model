{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['customer_key'], 'type': 'btree'},
            {'columns': ['source_company'], 'type': 'btree'}
        ]
    )
}}

with customers as (
    select * from {{ ref('int_all_customers') }}
),

customer_orders as (
    select
        customer_key,
        count(*) as total_orders,
        sum(amount_total) as total_revenue,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        avg(amount_total) as avg_order_value
    from {{ ref('fct_sales') }}
    group by 1
),

final as (
    select
        -- Keys
        c.customer_key,
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

        -- Order metrics
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.total_revenue, 0) as total_revenue,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.avg_order_value, 0) as avg_order_value,

        -- Customer segmentation
        case
            when co.total_revenue >= 100000 then 'Enterprise'
            when co.total_revenue >= 50000 then 'Mid-Market'
            when co.total_revenue >= 10000 then 'SMB'
            when co.total_revenue > 0 then 'Small'
            else 'Prospect'
        end as customer_segment,

        -- Status
        c.is_active,
        c.created_at,
        c.updated_at,
        current_timestamp as loaded_at

    from customers c
    left join customer_orders co on c.customer_key = co.customer_key
)

select * from final
