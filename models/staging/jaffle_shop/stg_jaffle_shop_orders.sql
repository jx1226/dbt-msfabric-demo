{{ config(
    incremental_strategy='insert_overwrite',
    materialized='table',
    file_format='delta',
    location_root='/dbfs/delta/'
) }}

with source_orders as (

    select * from {{ source('default', 'orders') }}

),

renamed_orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source_orders

)

select * from renamed_orders
