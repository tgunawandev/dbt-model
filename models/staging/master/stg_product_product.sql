{{
    config(
        materialized='view'
    )
}}

-- Union products from all companies
with tln_products as (
    select p.*, 'tln' as source_company
    from {{ source('tln_db', 'product_product') }} p
),

tmi_products as (
    select p.*, 'tmi' as source_company
    from {{ source('tmi_db', 'product_product') }} p
),

ieg_products as (
    select p.*, 'ieg' as source_company
    from {{ source('ieg_db', 'product_product') }} p
),

unioned as (
    select * from tln_products
    union all
    select * from tmi_products
    union all
    select * from ieg_products
),

renamed as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as product_key,

        -- Natural keys
        id as product_id,
        source_company,
        product_tmpl_id as product_template_id,

        -- Identifiers
        default_code as sku,
        barcode,

        -- Status
        active as is_active,

        -- Dates
        create_date as created_at,
        write_date as updated_at

    from unioned
    where active = true
)

select * from renamed
