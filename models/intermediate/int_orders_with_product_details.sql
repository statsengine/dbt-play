{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

-- Step 1: Identify new orders
with new_orders as (

    select
        o.order_id,
        o.user_id,
        o.product_id,
        o.quantity,
        o.base_amount,
        o.discount_percentage,
        o.discount_amount,
        o.total_amount,        -- Updated field name
        o.order_timestamp,
        o.order_date,
        o.order_hourly
    from {{ ref('stg_orders') }} as o
    {% if is_incremental() %}
        left join {{ this }} as existing on o.order_id = existing.order_id
        where existing.order_id is null
    {% endif %}
),

-- Step 2: Enhance orders with product data
enhanced_orders as (

    select
        o.order_id,
        o.user_id,
        o.product_id,
        o.quantity,
        p.material,
        p.size,
        p.style,
        p.rating,
        p.color,
        o.base_amount,
        o.discount_percentage,
        o.discount_amount,
        o.total_amount,         -- Updated field name
        o.order_timestamp,
        p.price,
        p.product_category,
        -- Calculated fields
        cast(o.order_date as timestamp) as order_date,
        -- Updated field name
        cast(o.order_hourly as timestamp) as order_hourly,
        p.price * o.quantity as total_product_amount,
        (p.price * o.quantity) - o.total_amount as discount_on_products
    from new_orders as o
    left join {{ ref('stg_products') }} as p on o.product_id = p.product_id
)

-- Final selection of fields
select
    order_id,
    user_id,
    product_id,
    quantity,
    base_amount,
    discount_percentage,
    discount_amount,
    total_amount,        -- Updated field name
    order_date,
    order_hourly,
    order_timestamp,
    price,
    product_category,
    total_product_amount,
    discount_on_products,
    material,
    size,
    style,
    rating,
    color
from enhanced_orders
