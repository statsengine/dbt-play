with product_metrics as (
    select
        o.product_id,
        p.product_category,
        p.brand,
        p.price,
        p.material,
        p.size,
        p.style,
        p.rating,
        p.color,
        SUM(quantity) as quantity,
        SUM(base_amount) as total_base_amount,
        AVG(discount_percentage) as avg_discount_percentage,
        SUM(total_amount) as total_amount
    from {{ ref('int_orders_with_product_details') }} as o
    left join {{ ref('stg_products') }} as p
        on o.product_id = p.product_id
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select * from product_metrics
