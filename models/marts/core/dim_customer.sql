{{
  config(
    materialized = 'table'
  )
}}

with users as (

  select 
    * 
  from {{ ref('int_users_visiting_dates') }}

),

final as (

  select
    *
  from users

)

select * from final