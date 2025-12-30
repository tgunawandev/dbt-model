{{
    config(
        materialized='view'
    )
}}

-- Union partners (customers/vendors) from all companies
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'res_partner') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'res_partner') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'res_partner') }}
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
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as partner_key,

        -- Natural keys
        id as partner_id,
        source_company,
        name as partner_name,
        display_name,

        -- Contact info
        email,
        phone,
        mobile,
        website,

        -- Address
        street,
        street2,
        city,
        zip as postal_code,
        country_id,
        state_id,

        -- Business info
        vat as tax_id,
        company_id,
        parent_id as parent_partner_id,
        commercial_partner_id,

        -- Classification
        customer_rank,
        supplier_rank,
        is_company,

        -- Status
        active as is_active,

        -- Dates
        create_date as created_at,
        write_date as updated_at

    from unioned
    where active = true
)

select * from renamed
