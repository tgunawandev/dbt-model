{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['account_key'], 'type': 'btree'},
            {'columns': ['account_code'], 'type': 'btree'},
            {'columns': ['account_type'], 'type': 'btree'}
        ]
    )
}}

-- Chart of Accounts dimension
-- Provides account classification for financial reporting

with accounts as (
    select * from {{ ref('stg_account_account') }}
),

-- Calculate account hierarchy level based on code pattern
enriched as (
    select
        account_key,
        account_id,
        source_company,
        account_code,
        account_name,

        -- Account classification
        account_type,
        internal_type,
        internal_group,

        -- Financial statement classification
        case
            when account_type in ('asset_receivable', 'asset_cash', 'asset_current', 'asset_non_current', 'asset_prepayments', 'asset_fixed')
                then 'Assets'
            when account_type in ('liability_payable', 'liability_credit_card', 'liability_current', 'liability_non_current')
                then 'Liabilities'
            when account_type in ('equity', 'equity_unaffected')
                then 'Equity'
            when account_type in ('income', 'income_other')
                then 'Revenue'
            when account_type in ('expense', 'expense_depreciation', 'expense_direct_cost')
                then 'Expenses'
            else 'Other'
        end as financial_statement_section,

        -- Balance sheet vs Income statement
        case
            when account_type in (
                'asset_receivable', 'asset_cash', 'asset_current', 'asset_non_current',
                'asset_prepayments', 'asset_fixed', 'liability_payable', 'liability_credit_card',
                'liability_current', 'liability_non_current', 'equity', 'equity_unaffected'
            ) then 'Balance Sheet'
            else 'Income Statement'
        end as statement_type,

        -- Debit/Credit nature
        case
            when account_type like 'asset%' or account_type like 'expense%'
                then 'Debit'
            else 'Credit'
        end as normal_balance,

        -- Properties
        is_reconcilable,
        is_deprecated,

        -- Hierarchy
        account_group_id,
        root_account_id,

        -- Account level (based on code length pattern)
        case
            when length(account_code) <= 2 then 1
            when length(account_code) <= 4 then 2
            when length(account_code) <= 6 then 3
            else 4
        end as account_level,

        -- Company
        company_id,

        -- Metadata
        created_at,
        updated_at,
        current_timestamp as loaded_at

    from accounts
)

select * from enriched
