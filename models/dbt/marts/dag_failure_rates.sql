{{
    config(
        materialized='table',
        tags=['marts', 'dag_metrics']
    )
}}

with dag_runs as (
    select * from {{ ref('stg_dag_runs') }}
),

failure_rates as (
    select
        dag_id,
        date_trunc('day', execution_date) as execution_day,
        count(*) as total_runs,
        count(case when state = 'failed' then 1 end) as failed_runs,
        count(case when state = 'success' then 1 end) as successful_runs,
        count(case when state in ('running', 'queued', 'scheduled') then 1 end) as in_progress_runs,
        round(
            (count(case when state = 'failed' then 1 end)::numeric / 
             nullif(count(*), 0)) * 100, 
            2
        ) as failure_rate_percent,
        round(
            (count(case when state = 'success' then 1 end)::numeric / 
             nullif(count(*), 0)) * 100, 
            2
        ) as success_rate_percent
    from dag_runs
    group by dag_id, date_trunc('day', execution_date)
),

rolling_metrics as (
    select
        dag_id,
        execution_day,
        total_runs,
        failed_runs,
        successful_runs,
        failure_rate_percent,
        success_rate_percent,
        -- 7-day rolling failure rate
        avg(failure_rate_percent) over (
            partition by dag_id 
            order by execution_day 
            rows between 6 preceding and current row
        ) as rolling_7d_failure_rate,
        -- 30-day rolling failure rate
        avg(failure_rate_percent) over (
            partition by dag_id 
            order by execution_day 
            rows between 29 preceding and current row
        ) as rolling_30d_failure_rate
    from failure_rates
)

select * from rolling_metrics
order by dag_id, execution_day desc

