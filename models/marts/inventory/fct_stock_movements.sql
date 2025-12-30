{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['stock_move_key'], 'type': 'btree'},
            {'columns': ['scheduled_date'], 'type': 'btree'},
            {'columns': ['source_company'], 'type': 'btree'},
            {'columns': ['product_key'], 'type': 'btree'}
        ]
    )
}}

-- Stock Movements Fact Table
-- Tracks inventory movements with link to GL for valuation

with moves as (
    select * from {{ ref('stg_stock_move') }}
),

products as (
    select * from {{ ref('stg_product_product') }}
),

final as (
    select
        -- Keys
        m.stock_move_key,
        m.stock_move_id,
        m.source_company,
        {{ dbt_utils.generate_surrogate_key(['m.source_company', 'm.product_id']) }} as product_key,

        -- Product info
        m.product_id,
        p.sku as product_sku,

        -- Movement details
        m.move_name,
        m.reference,
        m.origin,

        -- Quantities
        m.quantity as quantity_planned,
        m.quantity_done as quantity_actual,

        -- Locations
        m.source_location_id,
        m.destination_location_id,

        -- Movement type (inferred)
        case
            when m.source_location_id = 8 then 'Incoming'  -- Typically Vendors location
            when m.destination_location_id = 8 then 'Outgoing'
            else 'Internal'
        end as movement_type,

        -- Picking
        m.picking_id,
        m.picking_type_id,

        -- Value
        m.price_unit,
        m.quantity_done * coalesce(m.price_unit, 0) as movement_value,

        -- Status
        m.move_state,

        -- Dates
        m.scheduled_date,
        m.date_deadline,
        date_trunc('month', m.scheduled_date) as movement_month,

        -- Company
        m.company_id,

        -- Metadata
        m.created_at,
        m.updated_at,
        current_timestamp as loaded_at

    from moves m
    left join products p
        on m.source_company = p.source_company
        and m.product_id = p.product_id
    where m.move_state = 'done'  -- Only completed movements
)

select * from final
