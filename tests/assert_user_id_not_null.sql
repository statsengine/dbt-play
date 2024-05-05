select countif(user_pseudo_id is null) as n_null,
from {{ ref("int_all_events") }}
having n_null > 0
