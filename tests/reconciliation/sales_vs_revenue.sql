-- Reconciliation Test: Sales Orders vs Revenue in GL
-- This test checks that total sales order amounts approximately match
-- the revenue recorded in the General Ledger

-- Returns rows where variance exceeds threshold (test fails if any rows returned)

with sales_by_company as (
    select
        source_company,
        sum(amount_total) as sales_total
    from {{ ref('fct_sales_orders') }}
    where order_state in ('sale', 'done')
    group by 1
),

gl_revenue_by_company as (
    select
        source_company,
        sum(credit - debit) as gl_revenue
    from {{ ref('fct_general_ledger') }}
    where financial_statement_section = 'Revenue'
    group by 1
),

comparison as (
    select
        coalesce(s.source_company, g.source_company) as source_company,
        coalesce(s.sales_total, 0) as sales_total,
        coalesce(g.gl_revenue, 0) as gl_revenue,
        abs(coalesce(s.sales_total, 0) - coalesce(g.gl_revenue, 0)) as variance,
        case
            when coalesce(s.sales_total, 0) = 0 then 0
            else abs(coalesce(s.sales_total, 0) - coalesce(g.gl_revenue, 0)) / s.sales_total * 100
        end as variance_pct
    from sales_by_company s
    full outer join gl_revenue_by_company g
        on s.source_company = g.source_company
)

-- Return rows where variance exceeds 5% (adjust threshold as needed)
select *
from comparison
where variance_pct > 5
