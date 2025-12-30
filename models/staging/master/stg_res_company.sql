{{
    config(
        materialized='view'
    )
}}

-- Union companies from all databases
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'res_company') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'res_company') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'res_company') }}
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
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as company_key,

        -- Natural keys
        id as company_id,
        source_company,
        name as company_name,

        -- Business info
        partner_id,
        currency_id,
        parent_id as parent_company_id,

        -- Dates
        create_date as created_at,
        write_date as updated_at

    from unioned
)

select * from renamed
