{{
    config(
        materialized='view'
    )
}}

-- Union sales order lines from all companies
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'sale_order_line') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'sale_order_line') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'sale_order_line') }}
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
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as order_line_key,

        -- Natural keys
        id as order_line_id,
        source_company,
        order_id as sale_order_id,

        -- Product
        product_id,
        product_uom as unit_of_measure_id,

        -- Quantities
        product_uom_qty as quantity_ordered,
        qty_delivered as quantity_delivered,
        qty_invoiced as quantity_invoiced,
        qty_to_invoice as quantity_to_invoice,

        -- Pricing
        price_unit as unit_price,
        discount as discount_percent,
        price_subtotal as subtotal,
        price_tax as tax_amount,
        price_total as total_amount,

        -- Status
        state as line_state,
        invoice_status as line_invoice_status,

        -- Description
        name as line_description,
        sequence as line_sequence,

        -- Dates
        create_date as created_at,
        write_date as updated_at

    from unioned
)

select * from renamed
