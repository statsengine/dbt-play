{{ config(materialized="table") }}

with
    traffic as (

        select
            device_id,
            locale_id,
            traffic_id,
            sum(n_events) as n_events,
            sum(n_users) as n_users,
            sum(n_events_strings) as n_events_strings
        from {{ ref("int_traffic") }} s
        left join {{ ref("dim_device") }} d on d.session_id = s.session_id
        left join {{ ref("dim_locale") }} l on l.session_id = s.session_id
        left join {{ ref("dim_traffic") }} t on t.session_id = s.session_id
        group by 1, 2, 3
    )

select *
from traffic
