with source as (
    select * from {{ source('tmi_db', 'res_partner') }}
),

renamed as (
    select
        id as partner_id,
        name as partner_name,
        display_name,
        email,
        phone,
        mobile,
        website,
        street,
        street2,
        city,
        zip as postal_code,
        country_id,
        state_id,
        vat as tax_id,
        company_id,
        parent_id as parent_partner_id,
        commercial_partner_id,
        customer_rank,
        supplier_rank,
        is_company,
        active as is_active,
        create_date as created_at,
        write_date as updated_at,
        '{{ var("tmi_company_code") }}' as source_company

    from source
    where active = true
)

select * from renamed
