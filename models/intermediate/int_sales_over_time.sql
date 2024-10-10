with sales_daily as (

    select
        date_trunc(o.order_timestamp, day) as order_date,
        sum(o.total_amount) as daily_revenue,
        count(distinct o.order_id) as daily_orders,
        sum(o.quantity) as daily_quantity_sold
    from {{ ref('stg_orders') }} as o
    group by order_date
),

sales_monthly as (

    select
        date_trunc(o.order_timestamp, month) as order_month,
        sum(o.total_amount) as monthly_revenue,
        count(distinct o.order_id) as monthly_orders,
        sum(o.quantity) as monthly_quantity_sold
    from {{ ref('stg_orders') }} as o
    group by order_month
)

select * from sales_daily
union all
select * from sales_monthly
