{{
    config(
        materialized='table'
    )
}}

-- Trial Balance Fact Table
-- Aggregated by account and period

with gl as (
    select * from {{ ref('fct_general_ledger') }}
),

trial_balance as (
    select
        -- Dimensions
        source_company,
        posting_year as fiscal_year,
        posting_month,
        account_key,
        account_id,
        account_code,
        account_name,
        account_type,
        financial_statement_section,
        statement_type,
        normal_balance,

        -- Aggregated amounts
        sum(debit) as total_debit,
        sum(credit) as total_credit,
        sum(balance) as net_balance,

        -- Calculated balance based on account type
        case
            when normal_balance = 'Debit'
                then sum(debit) - sum(credit)
            else sum(credit) - sum(debit)
        end as account_balance,

        -- Transaction counts
        count(*) as transaction_count,
        count(distinct move_id) as journal_entry_count,

        -- Multi-currency (if single currency)
        sum(amount_currency) as total_amount_currency,

        current_timestamp as loaded_at

    from gl
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

with_running_balance as (
    select
        *,
        -- Running balance within fiscal year
        sum(account_balance) over (
            partition by source_company, account_key, fiscal_year
            order by posting_month
        ) as ytd_balance

    from trial_balance
)

select * from with_running_balance
order by source_company, account_code, posting_month
