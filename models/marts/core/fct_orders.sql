{{ config(materialized="table") }}

with
    orders as (

        select
            event_timestamp,
            item_id,
            item_name,
            item_brand,
            item_category,
            item_variant,
            sum(cast(o.price as int64)) as price,
            sum(cast(item_revenue as int64)) as revenue,
            sum(cast(quantity as int64)) as quantity,
            sum(cast(total_item_quantity as int64)) as total_item_quantity,
        from {{ ref("int_orders") }} o
        left join {{ ref("int_items") }} i on o.transaction_id = i.transaction_id
        group by 1, 2, 3, 4, 5, 6
    )

select *
from orders
