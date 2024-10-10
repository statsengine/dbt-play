with customer_orders as (

    select
        o.user_id,
        sum(o.total_amount) as total_spent,
        count(distinct o.order_id) as total_orders,
        min(o.order_timestamp) as first_order_date,
        max(o.order_timestamp) as last_order_date
    from {{ ref('stg_orders') }} as o
    group by o.user_id

),

customer_lifetime_value as (

    select
        c.user_id,
        c.user_name,
        c.email,
        c.age,
        c.sex,
        c.city,
        co.total_spent,
        co.total_orders,
        co.first_order_date,
        co.last_order_date,
        date_diff(
            cast(co.last_order_date as date),
            cast(co.first_order_date as date),
            day
        ) as customer_lifetime_days,
        co.total_spent / co.total_orders as average_order_value
    from {{ ref('stg_customers') }} as c
    left join customer_orders as co on c.user_id = co.user_id
)

select * from customer_lifetime_value
