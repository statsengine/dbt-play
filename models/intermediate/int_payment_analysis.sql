with payment_analysis as (

    select
        t.payment_method,
        count(distinct t.transaction_id) as total_transactions,
        sum(t.payment_amount) as total_payment_amount,
        avg(t.payment_amount) as average_payment_amount,
        count(distinct t.user_id) as unique_users
    from {{ ref('stg_transactions') }} as t
    group by t.payment_method

)

select * from payment_analysis
