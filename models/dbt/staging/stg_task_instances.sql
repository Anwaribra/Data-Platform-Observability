{{
    config(
        materialized='view',
        tags=['staging', 'task_instances']
    )
}}

with source as (
    select * from {{ source('observability', 'task_instances') }}
),

renamed as (
    select
        dag_id,
        task_id,
        execution_date,
        state,
        start_date,
        end_date,
        duration,
        try_number,
        extracted_at,

        coalesce(
            duration,
            extract(epoch from (end_date - start_date))
        ) as calculated_duration
    from source
)

select * from renamed

