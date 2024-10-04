select
    o.order_date,
    COUNT(distinct o.order_id) as total_orders,
    COUNT(distinct o.user_id) as total_users,
    COUNT(distinct o.product_id) as total_products,
    AVG(o.quantity) as avg_quantity,
    SUM(o.quantity) as total_quantity,
    AVG(o.base_amount) as avg_base_amount,
    SUM(o.base_amount) as total_base_amount,
    AVG(o.discount_percentage) as avg_discount_percentage,
    SUM(o.amount) as total_amount,
    AVG(o.amount) as avg_amount

from {{ ref('int_orders_with_product_details') }} as o
group by 1
