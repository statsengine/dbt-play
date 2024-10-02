with source as (
    select * from {{ source('ecommerce', 'ecommerce_orders') }}
),

renamed as (
    select
        user_id,
        order_id,
        product_id,
        quantity,
        base_amount,
        discount_percentage,
        amount,
        FORMAT_DATE('%Y-%m-%d', order_date) as order_date,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', order_timestamp) as order_hourly
    from source
)

select * from renamed
