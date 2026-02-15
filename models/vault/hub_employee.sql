{{ config(materialized='table', schema='vault') }}

with source_data as (

    select distinct
        employee_id,
        load_dts,
        record_source
    from {{ ref('stg_workday_employee') }}
    where employee_id is not null

),

hashed as (

    select
        to_hex(md5(employee_id)) as employee_hk,
        employee_id as employee_bk,
        load_dts,
        record_source
    from source_data

)

select * from hashed
