select
    cast(order_id as integer)    as order_id,
    cast(customer_id as integer) as customer_id,
    cast(order_date as date)     as order_date,
    cast(amount as double)       as amount,
    lower(trim(status))          as status
from {{ source('raw', 'orders') }}
where order_id is not null