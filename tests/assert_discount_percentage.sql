select *
from {{ ref('stg_orders') }}
where discount_percentage < 0
   or discount_percentage > 100