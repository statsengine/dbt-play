with source as (
    select * from {{ source('ecommerce', 'ecommerce_customers') }}
),

customer_data as (
    select
        user_id,
        user_name,
        email,
        age,
        sex,
        address,
        city
    from source
),

city_population_data as (
    select
        city,
        population
    from {{ ref('city_populations') }}  -- Reference the seed as a table
)

select
    cd.*,
    cp.population  -- Add population from the seed
from customer_data as cd
left join city_population_data as cp
    on cd.city = cp.city
