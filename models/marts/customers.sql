with customer_metrics as (
    select
        o.user_id,
        count(distinct o.order_id) as total_orders,
        sum(o.quantity) as total_quantity,
        sum(o.base_amount) as total_base_amount,
        sum(o.discount_amount) as total_discount, 
        sum(o.amount) as total_amount_paid,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date
    from {{ ref('int_orders_with_product_details') }} o
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
    cm.total_orders,
    cm.total_quantity,
    cm.total_base_amount,
    cm.total_discount,
    cm.total_amount_paid,
    cm.first_order_date,
    cm.last_order_date
from {{ ref('int_customers_with_order_dates') }} c
left join customer_metrics cm on c.user_id = cm.user_id