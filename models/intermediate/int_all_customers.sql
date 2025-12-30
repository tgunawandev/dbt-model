{{
    config(
        materialized='ephemeral'
    )
}}

-- Union all company customers into a single view

with tln_customers as (
    select * from {{ ref('stg_tln__customers') }}
),

tmi_customers as (
    select * from {{ ref('stg_tmi__customers') }}
),

ieg_customers as (
    select * from {{ ref('stg_ieg__customers') }}
),

all_customers as (
    select * from tln_customers
    union all
    select * from tmi_customers
    union all
    select * from ieg_customers
)

select
    -- Generate a globally unique customer key
    {{ dbt_utils.generate_surrogate_key(['source_company', 'partner_id']) }} as customer_key,
    *
from all_customers
