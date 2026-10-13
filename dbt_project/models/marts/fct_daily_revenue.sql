select
    order_date,
    count(*)               as total_orders,
    round(sum(amount), 2)  as total_revenue
from {{ ref('stg_orders') }}
where status = 'completed'
group by order_date
order by order_date