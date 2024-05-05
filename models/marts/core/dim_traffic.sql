{{ config(materialized="table") }}

with traffic as (select * from {{ ref("int_traffic") }})

select *
from traffic
