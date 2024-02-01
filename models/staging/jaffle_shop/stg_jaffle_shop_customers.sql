{{ config(
    materialized='view',
) }}

with source_customers as (

    select * from {{ source('ecom', 'customers') }}

),

renamed_customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from source_customers

)

select * from renamed_customers
