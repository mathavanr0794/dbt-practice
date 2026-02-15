{{ config(materialized='view', schema='staging') }}

select
    trim(employee_id) as employee_id,
    trim(manager_id) as manager_id,
    upper(first_name) as first_name,
    upper(last_name) as last_name,
    job_title,
    department,
    salary,
    hire_date,
    termination_date,
    location,
    last_updated_ts,
    ingestion_timestamp as load_dts,
    'WORKDAY' as record_source
from {{ source('raw', 'workday_employee') }}
