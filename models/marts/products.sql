with product_metrics AS (
    select 
        o.product_id,
        p.product_category,
        p.price,
        SUM(quantity) AS quantity,
        SUM(base_amount) AS total_base_amount,
        AVG(discount_percentage) AS avg_discount_percentage,
        SUM(amount) AS total_amount
    from {{ ref('int_orders_with_product_details') }} o
    left join {{ ref('stg_products') }} p
    on o.product_id = p.product_id
    group by 1, 2, 3
)

select * from product_metrics