with products as (
    select * from {{ source('tln_db', 'product_product') }}
),

templates as (
    select * from {{ source('tln_db', 'product_template') }}
),

joined as (
    select
        p.id as product_id,
        p.product_tmpl_id as product_template_id,
        t.name as product_name,
        p.default_code as sku,
        p.barcode,

        -- Product info
        t.type as product_type,
        t.categ_id as category_id,
        t.uom_id as unit_of_measure_id,

        -- Pricing
        t.list_price,
        t.standard_price as cost_price,

        -- Inventory
        t.tracking as inventory_tracking,

        -- Status
        p.active as is_active,
        t.sale_ok as is_saleable,
        t.purchase_ok as is_purchasable,

        -- Dates
        p.create_date as created_at,
        p.write_date as updated_at,

        -- Source identifier
        '{{ var("tln_company_code") }}' as source_company

    from products p
    left join templates t on p.product_tmpl_id = t.id
    where p.active = true
)

select * from joined
