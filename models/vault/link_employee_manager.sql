{{ config(
    materialized='table',
    schema='vault'
) }}

with source_data as (

    select distinct
        employee_id,
        manager_id,
        load_dts,
        record_source
    from {{ ref('stg_workday_employee') }}
    where employee_id is not null
      and manager_id is not null

),

hashed as (

    select
        to_hex(md5(concat(employee_id, manager_id))) as employee_manager_hk,
        to_hex(md5(employee_id)) as employee_hk,
        to_hex(md5(manager_id)) as manager_hk,
        load_dts,
        record_source
    from source_data

)

select * from hashed
