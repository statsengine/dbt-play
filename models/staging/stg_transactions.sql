with source as (
    select * from {{ source('ecommerce', 'ecommerce_transactions') }}
),

-- Standardize and enhance transaction data
enhanced as (
    select
        user_id,
        transaction_id,
        order_id,
        payment_date,
        cast(payment_date as timestamp) as payment_timestamp,
        payment_amount,
        lower(payment_method) as payment_method,
        lower(payment_status) as payment_status,
        format_date('%Y-%m-%d', payment_date) as formatted_payment_date
    from source
),

-- Validate against orders
validated_transactions as (
    select et.*
    from enhanced as et
    left join {{ ref('stg_orders') }} as o on et.order_id = o.order_id
    where o.order_id is not null
)

select * from validated_transactions
