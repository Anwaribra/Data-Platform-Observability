# Data Platform Observability 

An **Airflow Observability Platform** designed to monitor, analyze, and improve the reliability of data pipelines. This system tracks **Airflow metadata itself** (DAG runs, task executions, failures, retries, and performance metrics) and transforms it into analytics-ready datasets for comprehensive observability.

##  Overview

Instead of processing business data, this platform focuses on **observing the orchestrator itself**. It extracts metadata from Airflow's internal database, loads it into a dedicated observability database, performs data quality checks, and transforms it into actionable analytics models.

### Key Benefits

- **Visibility**: Track all DAG and task executions in one place
- **Performance Analysis**: Identify slow tasks and optimize pipeline performance
- **Failure Tracking**: Monitor failure rates and trends over time
- **SLA Monitoring**: Track SLA misses and ensure service level agreements are met
- **Data Quality**: Automated validation ensures data integrity
- **Historical Analysis**: Long-term trend analysis for capacity planning

##  Features

### Data Extraction
- **Automated Metadata Extraction**: Daily extraction of DAG runs and task instances from Airflow's metadata database
- **Relevant Fields**: Focuses on key metrics (dag_id, task_id, execution_date, state, duration, try_number)
- **Incremental Loading**: Efficient daily loads with date filtering

### Data Quality
- **Great Expectations Integration**: Comprehensive validation suites for data quality
- **Null Value Checks**: Ensures critical fields are never null
- **Value Range Validation**: Validates state values, duration ranges, and try numbers
- **Relationship Checks**: Validates date relationships (start_date ≤ end_date)

### Analytics Models (dbt)
- **DAG Runtime Metrics**: Average, median, P95 duration statistics per DAG
- **Failure Rate Analysis**: Daily failure rates with 7-day and 30-day rolling averages
- **Slowest Tasks**: Top 10 slowest tasks ranked by average duration
- **SLA Miss Tracking**: Identifies when DAGs or tasks exceed defined thresholds

### Production Ready
- **Error Handling**: Comprehensive error handling and logging
- **Retries**: Configurable retry logic for failed tasks
- **Monitoring**: Built-in logging and observability
- **Documentation**: Complete documentation with schema tests


## Data Pipeline Workflow

1. **Extraction**: Airflow DAG (`airflow_observability_pipeline`) runs daily at 2:00 AM UTC
2. **Metadata Collection**: Extracts `dag_run` and `task_instance` data from Airflow's metadata database
3. **Load**: Raw metadata is loaded into observability PostgreSQL database
4. **Quality Checks**: Data quality validation using Great Expectations
5. **Transformation**: dbt models transform raw data into analytics-ready tables
6. **Analytics**: Mart models provide insights on runtime, failures, and SLA misses
7. **Visualization**: Connect dashboards (Metabase, Tableau, Grafana) to analytics tables



##  Key Components

### 1. Extraction Module (`extract/airflow_metadata.py`)

Extracts relevant fields from Airflow's metadata database:
- **dag_runs**: dag_id, execution_date, state, start_date, end_date, duration
- **task_instances**: dag_id, task_id, execution_date, state, start_date, end_date, duration, try_number

**Features:**
- Calculates duration when missing
- Date filtering for incremental loads
- Automatic table creation
- Error handling and logging

### 2. Data Quality Checks

#### Basic Checks (`quality/data_quality_checks.py`)
- Table existence validation
- Row count checks
- Null value validation
- Data freshness checks

#### Great Expectations (`quality/expectations/`)
- **dag_runs_expectations.py**: Validates DAG run data
  - No null values in dag_id, execution_date
  - Valid state values
  - Duration >= 0
  - Date relationship validation

- **task_instances_expectations.py**: Validates task instance data
  - No null values in dag_id, task_id, execution_date
  - Valid state values
  - Duration >= 0, try_number >= 1
  - Date relationship validation

### 3. Analytics Models (dbt)

#### Staging Layer
- **stg_dag_runs**: Cleaned DAG run data with calculated duration
- **stg_task_instances**: Cleaned task instance data with calculated duration

#### Marts Layer
- **dag_runtime_metrics**: Daily aggregated metrics per DAG
  - Average, min, max, median, P95 duration
  - Success/failure counts
  
- **dag_failure_rates**: Daily failure rates with rolling averages
  - Daily failure and success rates
  - 7-day and 30-day rolling averages
  
- **slowest_tasks**: Top 10 slowest tasks
  - Ranked by average duration
  - Includes execution counts and failure rates
  
- **sla_misses**: SLA miss tracking
  - Tracks DAGs and tasks exceeding thresholds
  - Configurable SLA thresholds per DAG type
