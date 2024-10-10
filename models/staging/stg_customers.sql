{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce', 'ecommerce_customers') }}
),

standardized_data as (
    select
        user_id,
        user_name,
        email,
        address,
        age,
        sex,
        city,
        behavioral_profile,
        registration_timestamp,
        lower(device_type) as device_type,
        lower(segment) as segment,
        format_date('%Y-%m-%d', registration_timestamp) as registration_date
    from source
),

customer_age_group as (
    select
        *,
        case
            when age < 18 then 'Under 18'
            when age between 18 and 24 then '18-24'
            when age between 25 and 34 then '25-34'
            when age between 35 and 44 then '35-44'
            when age between 45 and 54 then '45-54'
            when age between 55 and 64 then '55-64'
            else '65+'
        end as age_group
    from standardized_data
),

city_population_data as (
    select
        city,
        population
    from {{ ref('city_populations') }}
)

select
    cd.*,
    cp.population
from customer_age_group as cd
left join city_population_data as cp
    on lower(cd.city) = lower(cp.city)
