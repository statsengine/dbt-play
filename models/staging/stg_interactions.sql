with source as (
    select * from {{ source('ecommerce', 'ecommerce_interactions') }}
),

standardized_data as (
    select
        user_id,
        product_id,
        interaction_timestamp,
        lower(interaction_type) as interaction_type,
        date(interaction_timestamp) as interaction_date,
        extract(hour from interaction_timestamp) as interaction_hour
    from source
)

select * from standardized_data
