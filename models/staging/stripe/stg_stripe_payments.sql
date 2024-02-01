{{ config(
    materialized='view',
) }}

with source_payments as (

    select * from {{ source('ecom', 'payments') }}

),

renamed_payments as (

    select
        id as payment_id,
        order_id as order_id,
        payment_method as payment_method,

        -- amount is stored in cents, convert it to dollars
        amount / 100 as amount

    from source_payments

)

select * from renamed_payments
