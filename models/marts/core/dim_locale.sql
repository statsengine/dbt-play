{{ config(materialized="table") }}

with
    traffic as (

        select
            row_number() over (order by language, country, city) as locale_id,
            session_id,
            language,
            country,
            city
        from {{ ref("int_traffic") }}

    )

select *
from traffic
