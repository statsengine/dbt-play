with source as (
    select * from {{ source('ecommerce', 'ecommerce_transactions') }}
),

renamed as (
    select
        transaction_id,
        order_id,
        payment_method,
        payment_status,
        FORMAT_DATE('%Y-%m-%d', payment_date) AS payment_date,
        payment_amount
    from source
)

select * from renamed   