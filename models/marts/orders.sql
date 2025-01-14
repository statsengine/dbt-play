select
    o.product_category,
    o.size,
    o.style,
    o.color,
    CAST(o.order_date as DATE) as order_date,
    CAST(o.order_timestamp as TIMESTAMP) as order_date_timestamp,
    DATE_TRUNC(CAST(o.order_date as DATE), week) as order_date_week,
    DATE_TRUNC(CAST(o.order_date as DATE), month) as order_date_month,
    DATE_TRUNC(CAST(o.order_date as DATE), quarter) as order_date_quarter,
    COUNT(distinct o.order_id) as total_orders,
    COUNT(distinct o.user_id) as total_users,
    COUNT(distinct o.product_id) as total_products,
    AVG(o.quantity) as avg_quantity,
    SUM(o.quantity) as total_quantity,
    AVG(o.base_amount) as avg_base_amount,
    SUM(o.base_amount) as total_base_amount,
    AVG(o.discount_percentage) as avg_discount_percentage,
    SUM(o.total_amount) as total_amount,
    AVG(o.total_amount) as avg_amount

from {{ ref('int_orders_with_product_details') }} as o
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
