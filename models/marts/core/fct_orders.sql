{{ config(
    materialized='view',
) }}

with orders as  (

    select * from {{ ref('stg_jaffle_shop_orders' )}}

),

payments as (

    select * from {{ ref('stg_stripe_payments') }}

),

order_payments as (

    select
        order_id,
        sum(amount) as amount

    from payments

    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        coalesce(order_payments.amount, 0) as amount

    from orders

    left join order_payments
    
    ON orders.order_id = order_payments.order_id

)

select * from final
