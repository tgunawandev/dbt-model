{{
    config(
        materialized='table'
    )
}}

-- Bridge Table: Sales Orders to General Ledger
-- Enables drill-through from sales to accounting entries
-- Use this for reconciliation and variance analysis

with orders as (
    select * from {{ ref('stg_sale_order') }}
),

gl_lines as (
    select * from {{ ref('fct_general_ledger') }}
),

-- Match sales orders to their invoice GL entries via invoice_origin
bridge as (
    select
        -- Sales order info
        o.sale_order_key,
        o.sale_order_id,
        o.order_number,
        o.source_company,
        o.order_date,
        o.amount_total as order_total,
        o.order_state,

        -- GL line info
        gl.gl_line_key,
        gl.gl_line_id,
        gl.move_key,
        gl.move_name,
        gl.move_type,
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
        gl.balance,

        -- Product (if on invoice line)
        gl.product_id,

        -- Partner
        gl.partner_id

    from orders o
    inner join gl_lines gl
        on o.source_company = gl.source_company
        and o.order_number = gl.invoice_origin
    where gl.move_type = 'out_invoice'
)

select * from bridge
