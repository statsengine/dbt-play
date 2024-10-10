with source as (
    select * from {{ source('ecommerce', 'ecommerce_orders') }}
),

enhanced as (
    select
        user_id,
        order_id,
        product_id,
        quantity,
        base_amount,
        discount_percentage,
        order_date,
        order_timestamp,
        base_amount * quantity * (discount_percentage / 100) as discount_amount,
        base_amount
        * quantity
        * (1 - discount_percentage / 100) as total_amount,
        format_date('%Y-%m-%d', order_date) as formatted_order_date,
        format_timestamp('%Y-%m-%d %H:00:00', order_timestamp) as order_hourly
    from source
)

select * from enhanced
