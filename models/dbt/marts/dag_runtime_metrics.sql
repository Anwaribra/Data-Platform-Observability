{{
    config(
        materialized='table',
        tags=['marts', 'dag_metrics']
    )
}}

with dag_runs as (
    select * from {{ ref('stg_dag_runs') }}
),

dag_runtime_metrics as (
    select
        dag_id,
        date_trunc('day', execution_date) as execution_day,
        count(*) as total_runs,
        count(case when state = 'success' then 1 end) as successful_runs,
        count(case when state = 'failed' then 1 end) as failed_runs,
        avg(calculated_duration) as avg_duration_seconds,
        min(calculated_duration) as min_duration_seconds,
        max(calculated_duration) as max_duration_seconds,
        percentile_cont(0.5) within group (order by calculated_duration) as median_duration_seconds,
        percentile_cont(0.95) within group (order by calculated_duration) as p95_duration_seconds,
        sum(calculated_duration) as total_duration_seconds
    from dag_runs
    where calculated_duration is not null
    group by dag_id, date_trunc('day', execution_date)
)

select * from dag_runtime_metrics

