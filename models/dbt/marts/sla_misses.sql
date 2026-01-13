{{
    config(
        materialized='table',
        tags=['marts', 'sla_metrics']
    )
}}

with dag_runs as (
    select * from {{ ref('stg_dag_runs') }}
),

task_instances as (
    select * from {{ ref('stg_task_instances') }}
),

-- Define SLA thresholds (in seconds)
-- These can be customized per DAG or task
sla_thresholds as (
    select
        dag_id,
        task_id,
        case
            when dag_id like '%critical%' then 3600  -- 1 hour for critical DAGs
            when dag_id like '%daily%' then 7200     -- 2 hours for daily DAGs
            else 10800                                -- 3 hours default
        end as sla_threshold_seconds
    from task_instances
    group by dag_id, task_id
),

task_sla_analysis as (
    select
        ti.dag_id,
        ti.task_id,
        date_trunc('day', ti.execution_date) as execution_day,
        count(*) as total_executions,
        count(case when ti.calculated_duration > st.sla_threshold_seconds then 1 end) as sla_misses,
        count(case when ti.calculated_duration <= st.sla_threshold_seconds then 1 end) as sla_meets,
        round(
            (count(case when ti.calculated_duration > st.sla_threshold_seconds then 1 end)::numeric /
             nullif(count(*), 0)) * 100,
            2
        ) as sla_miss_rate_percent,
        avg(ti.calculated_duration) as avg_duration_seconds,
        max(ti.calculated_duration) as max_duration_seconds,
        st.sla_threshold_seconds
    from task_instances ti
    inner join sla_thresholds st
        on ti.dag_id = st.dag_id
        and ti.task_id = st.task_id
    where ti.calculated_duration is not null
    group by ti.dag_id, ti.task_id, date_trunc('day', ti.execution_date), st.sla_threshold_seconds
),

dag_sla_analysis as (
    select
        dr.dag_id,
        date_trunc('day', dr.execution_date) as execution_day,
        count(*) as total_runs,
        count(case when dr.calculated_duration > 7200 then 1 end) as sla_misses,  
        count(case when dr.calculated_duration <= 7200 then 1 end) as sla_meets,
        round(
            (count(case when dr.calculated_duration > 7200 then 1 end)::numeric /
             nullif(count(*), 0)) * 100,
            2
        ) as sla_miss_rate_percent,
        avg(dr.calculated_duration) as avg_duration_seconds,
        max(dr.calculated_duration) as max_duration_seconds
    from dag_runs dr
    where dr.calculated_duration is not null
    group by dr.dag_id, date_trunc('day', dr.execution_date)
)

-- Combine task and DAG SLA misses
select
    'task' as entity_type,
    dag_id,
    task_id as identifier,
    execution_day,
    total_executions as total_count,
    sla_misses,
    sla_meets,
    sla_miss_rate_percent,
    avg_duration_seconds,
    max_duration_seconds,
    sla_threshold_seconds
from task_sla_analysis

union all

select
    'dag' as entity_type,
    dag_id,
    null as identifier,
    execution_day,
    total_runs as total_count,
    sla_misses,
    sla_meets,
    sla_miss_rate_percent,
    avg_duration_seconds,
    max_duration_seconds,
    7200 as sla_threshold_seconds
from dag_sla_analysis

order by execution_day desc, sla_miss_rate_percent desc

