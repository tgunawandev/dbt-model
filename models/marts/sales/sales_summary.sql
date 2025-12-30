{{
    config(
        materialized='table'
    )
}}

-- Daily sales summary aggregated by company

with daily_sales as (
    select
        source_company,
        date_trunc('day', order_date)::date as sales_date,
        count(*) as order_count,
        count(distinct customer_key) as unique_customers,
        sum(amount_untaxed) as revenue_untaxed,
        sum(amount_tax) as tax_amount,
        sum(amount_total) as total_revenue,
        avg(amount_total) as avg_order_value,
        min(amount_total) as min_order_value,
        max(amount_total) as max_order_value
    from {{ ref('fct_sales') }}
    where order_status in ('sale', 'done')
    group by 1, 2
),

with_comparisons as (
    select
        *,
        -- Previous day comparison
        lag(total_revenue) over (
            partition by source_company
            order by sales_date
        ) as prev_day_revenue,

        -- Same day last week
        lag(total_revenue, 7) over (
            partition by source_company
            order by sales_date
        ) as same_day_last_week_revenue,

        -- Running totals
        sum(total_revenue) over (
            partition by source_company, date_trunc('month', sales_date)
            order by sales_date
        ) as mtd_revenue,

        sum(total_revenue) over (
            partition by source_company, date_trunc('year', sales_date)
            order by sales_date
        ) as ytd_revenue

    from daily_sales
)

select
    source_company,
    sales_date,
    order_count,
    unique_customers,
    revenue_untaxed,
    tax_amount,
    total_revenue,
    avg_order_value,
    min_order_value,
    max_order_value,
    prev_day_revenue,
    same_day_last_week_revenue,
    mtd_revenue,
    ytd_revenue,

    -- Growth metrics
    case
        when prev_day_revenue > 0
        then round(((total_revenue - prev_day_revenue) / prev_day_revenue * 100)::numeric, 2)
        else null
    end as day_over_day_growth_pct,

    case
        when same_day_last_week_revenue > 0
        then round(((total_revenue - same_day_last_week_revenue) / same_day_last_week_revenue * 100)::numeric, 2)
        else null
    end as week_over_week_growth_pct,

    current_timestamp as loaded_at

from with_comparisons
order by source_company, sales_date desc
