{{ config(materialized="view") }}

with
    items as (

        select
            dense_rank() over (
                order by item_id, item_name, item_brand, item_variant, item_category
            ) as items_id,
            item_id,
            item_name,
            item_brand,
            item_variant,
            item_category,
            price,
            transaction_id,
        from {{ ref("stg_google_analytics_events") }}
        where item_id is not null
        group by 2, 3, 4, 5, 6, 7, 8
    )

select *
from items
