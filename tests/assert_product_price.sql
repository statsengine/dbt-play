select *
from {{ ref('stg_products') }}
where price < 0