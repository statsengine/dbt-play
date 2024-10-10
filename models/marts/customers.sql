with customer_metrics as (
    select
        o.user_id,
        count(distinct o.order_id) as total_orders,
        sum(o.quantity) as total_quantity,
        sum(o.base_amount) as total_base_amount,
        sum(o.discount_amount) as total_discount,
        sum(o.total_amount) as total_amount_paid,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date
    from {{ ref('int_orders_with_product_details') }} as o
    group by o.user_id
)

select
    c.user_id,
    c.user_name,
    c.email,
    c.age,
    c.sex,
    c.address,
    c.city,
    cm.first_order_date,
    cm.last_order_date,
    coalesce(cm.total_orders, 0) as total_orders,
    coalesce(cm.total_quantity, 0) as total_quantity,
    coalesce(cm.total_base_amount, 0) as total_base_amount,
    coalesce(cm.total_discount, 0) as total_discount,
    coalesce(cm.total_amount_paid, 0) as total_amount_paid
from {{ ref('int_customers_with_order_dates') }} as c
left join customer_metrics as cm on c.user_id = cm.user_id
