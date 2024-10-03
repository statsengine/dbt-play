with new_orders as (
   select *
   from {{ ref('stg_orders') }}
   {% if is_incremental() %}
    where order_id not in (select order_id from {{ this }})
   {% endif %} 
),

orders as (
    select
        o.order_id,
        o.user_id,
        o.product_id,
        o.quantity,
        o.base_amount,
        o.discount_percentage,
        o.base_amount * (o.discount_percentage / 100.0) AS discount_amount,
        o.amount,
        o.order_date,
        o.order_hourly,
        p.price,
        p.product_category,
        p.price * o.quantity as total_amount_products,
        o.amount - p.price * o.quantity as discount_products
    from new_orders o
    left join {{ ref('stg_products') }} p on o.product_id = p.product_id
)

select
    order_id,
    user_id,
    product_id,
    quantity,
    base_amount,
    discount_percentage,
    discount_amount,
    amount,
    order_date,
    order_hourly,
    price,
    product_category,
    total_amount_products,
    discount_products
from orders