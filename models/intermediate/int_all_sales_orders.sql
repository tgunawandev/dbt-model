{{
    config(
        materialized='ephemeral'
    )
}}

-- Union all company sales orders into a single view

with tln_orders as (
    select * from {{ ref('stg_tln__sales_orders') }}
),

tmi_orders as (
    select * from {{ ref('stg_tmi__sales_orders') }}
),

ieg_orders as (
    select * from {{ ref('stg_ieg__sales_orders') }}
),

all_orders as (
    select * from tln_orders
    union all
    select * from tmi_orders
    union all
    select * from ieg_orders
)

select
    -- Generate a globally unique order ID
    {{ dbt_utils.generate_surrogate_key(['source_company', 'sale_order_id']) }} as order_key,
    *
from all_orders
