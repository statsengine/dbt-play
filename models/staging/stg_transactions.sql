with source as (
    select * from {{ source('ecommerce', 'ecommerce_transactions') }}
),

renamed as (
    select
        transaction_id,
        order_id,
        payment_method,
        payment_status,
        payment_date,
        payment_amount
    from source
)

select * from renamed   