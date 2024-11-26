select
    p.id as payment_id,
    p.order_id,
    p.payment_method,
    
    p.amount/100 as amount,
    o.status

from dbt_jaffle.raw_payments as p
join {{ ref('stg_jaffle_shop__orders') }} as o
on p.order_id = o.order_id