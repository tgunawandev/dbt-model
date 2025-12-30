{{
    config(
        materialized='view'
    )
}}

-- Union sales orders from all companies
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'sale_order') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'sale_order') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'sale_order') }}
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
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as sale_order_key,

        -- Natural keys
        id as sale_order_id,
        source_company,
        name as order_number,

        -- Status
        state as order_state,
        invoice_status,

        -- Dates
        date_order as order_date,
        validity_date,
        commitment_date,
        create_date as created_at,
        write_date as updated_at,

        -- Customer
        partner_id as customer_id,
        partner_invoice_id,
        partner_shipping_id,

        -- Sales info
        user_id as salesperson_id,
        team_id as sales_team_id,

        -- Company/warehouse
        company_id,
        warehouse_id,

        -- Pricing
        pricelist_id,
        currency_rate,

        -- Amounts
        amount_untaxed,
        amount_tax,
        amount_total,

        -- References
        origin,
        client_order_ref as customer_reference,

        -- Analytics
        analytic_account_id

    from unioned
    where state != 'cancel'
)

select * from renamed
