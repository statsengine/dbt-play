{{ config(materialized="view") }}

with
    orders as (

        select
            dense_rank() over (
                order by
                    transaction_id, price, quantity, total_item_quantity, item_revenue
            ) as order_id,
            transaction_id,
            event_timestamp,
            price,
            quantity,
            total_item_quantity,
            item_revenue
        from {{ ref("stg_google_analytics_events") }}
        where transaction_id is not null
        group by 2, 3, 4, 5, 6, 7
    )

select *
from orders
