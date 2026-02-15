{{ config(
    materialized='table',
    schema='vault'
) }}

with source_data as (

    select distinct
        manager_id,
        load_dts,
        record_source
    from {{ ref('stg_workday_employee') }}
    where manager_id is not null

),

hashed as (

    select
        to_hex(md5(manager_id)) as manager_hk,
        manager_id as manager_bk,
        load_dts,
        record_source
    from source_data

)

select * from hashed
