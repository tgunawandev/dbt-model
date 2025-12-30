{{
    config(
        materialized='view'
    )
}}

-- Union journal entries from all companies
with tln as (
    select *, 'tln' as source_company
    from {{ source('tln_db', 'account_move') }}
),

tmi as (
    select *, 'tmi' as source_company
    from {{ source('tmi_db', 'account_move') }}
),

ieg as (
    select *, 'ieg' as source_company
    from {{ source('ieg_db', 'account_move') }}
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
        -- Surrogate key (globally unique)
        {{ dbt_utils.generate_surrogate_key(['source_company', 'id']) }} as move_key,

        -- Natural keys
        id as move_id,
        source_company,
        name as move_name,

        -- Journal entry type
        move_type,
        state as move_state,

        -- Dates
        date as posting_date,
        invoice_date,
        invoice_date_due as due_date,
        create_date as created_at,
        write_date as updated_at,

        -- References
        ref as reference,
        invoice_origin,
        payment_reference,

        -- Relationships
        journal_id,
        partner_id,
        commercial_partner_id,
        company_id,
        currency_id,
        invoice_user_id as responsible_user_id,

        -- Amounts
        amount_untaxed,
        amount_tax,
        amount_total,
        amount_residual as amount_due,
        amount_untaxed_signed,
        amount_tax_signed,
        amount_total_signed,
        amount_residual_signed as amount_due_signed,

        -- Payment
        payment_state,

        -- Flags
        to_check as needs_review

    from unioned
    where state = 'posted'  -- Only posted entries for financial truth
)

select * from renamed
