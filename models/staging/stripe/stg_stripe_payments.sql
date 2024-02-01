{{ config(
    incremental_strategy='insert_overwrite',
    materialized='table',
    file_format='delta',
    location_root='/dbfs/delta/'
) }}

with source_payments as (

    select * from {{ source('default', 'payments') }}

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
