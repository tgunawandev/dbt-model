with source as (
    select * from {{ source('tln_db', 'account_move') }}
),

renamed as (
    select
        id as invoice_id,
        name as invoice_number,

        -- Type and status
        move_type as invoice_type,
        state as invoice_status,
        payment_state,

        -- Partners
        partner_id,
        commercial_partner_id,

        -- Company
        company_id,
        journal_id,
        currency_id,

        -- Dates
        invoice_date,
        invoice_date_due as due_date,
        date as accounting_date,
        create_date as created_at,
        write_date as updated_at,

        -- Amounts
        amount_untaxed,
        amount_tax,
        amount_total,
        amount_residual as amount_due,

        -- References
        ref as reference,
        invoice_origin,

        -- Source identifier
        '{{ var("tln_company_code") }}' as source_company

    from source
    where move_type in ('out_invoice', 'out_refund', 'in_invoice', 'in_refund')
      and state != 'cancel'
)

select * from renamed
