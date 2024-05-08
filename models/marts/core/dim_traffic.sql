{{ config(materialized="table") }}

with
    traffic as (

        select
            dense_rank() over (
                order by session_id, traffic_medium, traffic_name, traffic_source
            ) as traffic_id,
            session_id,
            traffic_medium,
            traffic_name,
            traffic_source
        from {{ ref("int_traffic") }}
        group by 2, 3, 4, 5
    )

select *
from traffic
