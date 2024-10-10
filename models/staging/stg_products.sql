{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce', 'ecommerce_products') }}
),

standardized_data as (
    select
        product_id,
        price,
        size,
        rating,
        lower(product_category) as product_category,
        lower(brand) as brand,
        lower(color) as color,
        lower(material) as material,
        lower(style) as style
    from source
)

select * from standardized_data
