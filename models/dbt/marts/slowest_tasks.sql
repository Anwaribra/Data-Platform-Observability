{{
    config(
        materialized='table',
        tags=['marts', 'task_metrics']
    )
}}

with task_instances as (
    select * from {{ ref('stg_task_instances') }}
),

task_metrics as (
    select
        dag_id,
        task_id,
        count(*) as total_executions,
        avg(calculated_duration) as avg_duration_seconds,
        min(calculated_duration) as min_duration_seconds,
        max(calculated_duration) as max_duration_seconds,
        percentile_cont(0.95) within group (order by calculated_duration) as p95_duration_seconds,
        sum(calculated_duration) as total_duration_seconds,
        count(case when state = 'failed' then 1 end) as failed_executions,
        count(case when state = 'success' then 1 end) as successful_executions,
        max(extracted_at) as last_updated
    from task_instances
    where calculated_duration is not null
    group by dag_id, task_id
),

ranked_tasks as (
    select
        dag_id,
        task_id,
        total_executions,
        avg_duration_seconds,
        max_duration_seconds,
        p95_duration_seconds,
        total_duration_seconds,
        failed_executions,
        successful_executions,
        round(
            (failed_executions::numeric / nullif(total_executions, 0)) * 100,
            2
        ) as failure_rate_percent,
        last_updated,
        row_number() over (
            order by avg_duration_seconds desc nulls last
        ) as rank_by_avg_duration,
        row_number() over (
            order by max_duration_seconds desc nulls last
        ) as rank_by_max_duration
    from task_metrics
)

select * from ranked_tasks
order by avg_duration_seconds desc nulls last
limit 10

