{{
    config(
        materialized='view',
        tags=['staging', 'dag_runs']
    )
}}

with source as (
    select * from {{ source('observability', 'dag_runs') }}
),

renamed as (
    select
        dag_id,
        execution_date,
        state,
        start_date,
        end_date,
        duration,
        extracted_at,
        coalesce(
            duration,
            extract(epoch from (end_date - start_date))
        ) as calculated_duration
    from source
)

select * from renamed

