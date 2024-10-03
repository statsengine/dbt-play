with transactions as (
    select 
        transaction_id,
        order_id,
        payment_method,
        payment_status,
        payment_date,
        payment_amount
    from {{ ref('stg_transactions') }}
),

aggregated as (
    select
        payment_date,
        payment_method,
        count(transaction_id) as total_transactions,
        sum(payment_amount) as total_revenue,
        avg(payment_amount) as avg_transaction_value,
    from transactions
    where payment_status = 'Success'
    group by 1, 2
)   

select * from aggregated