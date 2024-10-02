with source as (
    select * from {{ source('ecommerce', 'ecommerce_products') }}
),

renamed as (
    select
        product_id,
        product_category,
        price
    from source
)

select * from renamed