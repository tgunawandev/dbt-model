with source as (
    select * from {{ source('tln_db', 'sale_order_line') }}
),

renamed as (
    select
        id as order_line_id,
        order_id as sale_order_id,
        product_id,

        -- Quantities
        product_uom_qty as quantity_ordered,
        qty_delivered as quantity_delivered,
        qty_invoiced as quantity_invoiced,
        qty_to_invoice as quantity_to_invoice,
        product_uom as unit_of_measure_id,

        -- Pricing
        price_unit as unit_price,
        price_subtotal as subtotal,
        price_tax as tax_amount,
        price_total as total_amount,
        discount as discount_percent,

        -- Dates
        create_date as created_at,
        write_date as updated_at,

        -- Status
        state as line_status,
        invoice_status as line_invoice_status,

        -- Display
        name as line_description,
        sequence as line_sequence,

        -- Source identifier
        '{{ var("tln_company_code") }}' as source_company

    from source
)

select * from renamed
