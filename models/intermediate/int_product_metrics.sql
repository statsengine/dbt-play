with product_sales as (

    select
        p.product_id,
        p.product_category,
        p.brand,
        p.price,
        p.color,
        p.style,
        p.material,
        p.size,
        sum(o.quantity) as total_quantity_sold,
        sum(o.total_amount) as total_revenue,
        avg(p.rating) as average_rating,
        count(distinct o.order_id) as total_orders
    from {{ ref('stg_orders') }} as o
    left join {{ ref('stg_products') }} as p on o.product_id = p.product_id
    group by
        p.product_id,
        p.product_category,
        p.brand,
        p.price,
        p.color,
        p.style,
        p.material,
        p.size

)

select * from product_sales
