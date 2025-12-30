{{
    config(
        materialized='table'
    )
}}

-- Bridge Table: Stock Movements to General Ledger
-- Links inventory movements to their accounting entries
-- Use for inventory valuation reconciliation

with stock_moves as (
    select * from {{ ref('fct_stock_movements') }}
),

gl_lines as (
    select * from {{ ref('fct_general_ledger') }}
    where stock_move_id is not null  -- Only GL lines linked to stock moves
),

bridge as (
    select
        -- Stock move info
        sm.stock_move_key,
        sm.stock_move_id,
        sm.source_company,
        sm.product_id,
        sm.product_sku,
        sm.movement_type,
        sm.quantity_actual,
        sm.movement_value as stock_move_value,
        sm.scheduled_date,

        -- GL line info
        gl.gl_line_key,
        gl.gl_line_id,
        gl.move_key,
        gl.move_name as journal_entry,
        gl.posting_date,

        -- Account info
        gl.account_key,
        gl.account_code,
        gl.account_name,
        gl.account_type,
        gl.financial_statement_section,

        -- Amounts
        gl.debit,
        gl.credit,
        gl.balance

    from stock_moves sm
    inner join gl_lines gl
        on sm.source_company = gl.source_company
        and sm.stock_move_id = gl.stock_move_id
)

select * from bridge
