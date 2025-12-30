{{
    config(
        materialized='view'
    )
}}

-- Union chart of accounts from all companies
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'account_account') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'account_account') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'account_account') }}
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
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as account_key,

        -- Natural keys
        id as account_id,
        source_company,
        code as account_code,
        name as account_name,  -- JSONB in Odoo 16+

        -- Classification
        account_type,
        internal_type,
        internal_group,

        -- Hierarchy
        group_id as account_group_id,
        root_id as root_account_id,

        -- Properties
        reconcile as is_reconcilable,
        deprecated as is_deprecated,
        currency_id,
        company_id,

        -- Dates
        create_date as created_at,
        write_date as updated_at

    from unioned
    where deprecated = false or deprecated is null
)

select * from renamed
