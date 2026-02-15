{{ config(materialized='incremental', schema='vault') }}

with source_data as (

    select
        employee_id,
        first_name,
        last_name,
        job_title,
        department,
        hire_date,
        load_dts,
        record_source
    from {{ ref('stg_workday_employee') }}

),

hashed as (

    select
        to_hex(md5(employee_id)) as employee_hk,

        -- Hash diff for change detection
        to_hex(md5(concat(
            coalesce(first_name, ''),
            coalesce(last_name, ''),
            coalesce(job_title, ''),
            coalesce(department, ''),
            coalesce(cast(hire_date as string), '')
        ))) as hash_diff,

        first_name,
        last_name,
        job_title,
        department,
        hire_date,
        load_dts,
        record_source

    from source_data
)

select * from hashed

{% if is_incremental() %}

where hash_diff not in (
    select hash_diff from {{ this }}
)

{% endif %}
