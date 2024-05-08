{{ config(materialized="table") }}

with
    traffic as (

        select
            row_number() over (
                order by device_category, mobile_brand_name, operating_system
            ) as device_id,
            session_id,
            device_category,
            mobile_brand_name,
            operating_system
        from {{ ref("int_traffic") }}

    )

select *
from traffic
