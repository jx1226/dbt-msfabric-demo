{{ config(
    incremental_strategy='insert_overwrite',
    materialized='table',
    file_format='delta',
    location_root='/dbfs/delta/'
) }}

with source_customers as (

    select * from {{ source('default', 'customers') }}

),

renamed_customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from source_customers

)

select * from renamed_customers
