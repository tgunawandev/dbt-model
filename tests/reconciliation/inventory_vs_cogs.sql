-- Reconciliation Test: Inventory Movements vs COGS in GL
-- This test checks that inventory valuation movements match
-- the Cost of Goods Sold recorded in the General Ledger

-- Returns rows where there's a significant mismatch

with stock_value_by_month as (
    select
        source_company,
        movement_month,
        sum(case when movement_type = 'Outgoing' then movement_value else 0 end) as outgoing_value,
        sum(case when movement_type = 'Incoming' then movement_value else 0 end) as incoming_value
    from {{ ref('fct_stock_movements') }}
    group by 1, 2
),

gl_cogs_by_month as (
    select
        source_company,
        posting_month,
        sum(debit - credit) as cogs_amount
    from {{ ref('fct_general_ledger') }}
    where account_type in ('expense_direct_cost', 'expense')
      and account_code like '5%'  -- Typically COGS accounts
    group by 1, 2
),

comparison as (
    select
        coalesce(s.source_company, g.source_company) as source_company,
        coalesce(s.movement_month, g.posting_month) as period,
        coalesce(s.outgoing_value, 0) as stock_outgoing,
        coalesce(g.cogs_amount, 0) as gl_cogs,
        abs(coalesce(s.outgoing_value, 0) - coalesce(g.cogs_amount, 0)) as variance
    from stock_value_by_month s
    full outer join gl_cogs_by_month g
        on s.source_company = g.source_company
        and s.movement_month = g.posting_month
)

-- Return rows where variance is significant
select *
from comparison
where variance > 10000  -- Adjust threshold as needed
order by source_company, period
