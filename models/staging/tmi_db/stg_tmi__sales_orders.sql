with source as (
    select * from {{ source('tmi_db', 'sale_order') }}
),

renamed as (
    select
        id as sale_order_id,
        name as order_number,
        partner_id as customer_id,
        user_id as salesperson_id,
        team_id as sales_team_id,
        company_id,
        warehouse_id,
        pricelist_id,
        analytic_account_id,

        date_order as order_date,
        validity_date as valid_until_date,
        commitment_date,
        create_date as created_at,
        write_date as updated_at,

        state as order_status,
        invoice_status,

        amount_untaxed,
        amount_tax,
        amount_total,
        currency_rate,

        origin,
        client_order_ref as customer_reference,

        '{{ var("tmi_company_code") }}' as source_company

    from source
    where state != 'cancel'
)

select * from renamed
