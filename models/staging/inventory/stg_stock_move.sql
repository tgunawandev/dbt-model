{{
    config(
        materialized='view'
    )
}}

-- Union stock movements from all companies
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'stock_move') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'stock_move') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'stock_move') }}
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
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as stock_move_key,

        -- Natural keys
        id as stock_move_id,
        source_company,
        name as move_name,
        reference,

        -- Product
        product_id,
        product_uom as unit_of_measure_id,
        product_uom_qty as quantity,
        quantity_done,

        -- Locations
        location_id as source_location_id,
        location_dest_id as destination_location_id,

        -- Picking
        picking_id,
        picking_type_id,

        -- Status
        state as move_state,

        -- Dates
        date as scheduled_date,
        date_deadline,
        create_date as created_at,
        write_date as updated_at,

        -- Value
        price_unit,

        -- Origin
        origin,
        group_id as procurement_group_id,

        -- Company
        company_id

    from unioned
    where state != 'cancel'
)

select * from renamed
