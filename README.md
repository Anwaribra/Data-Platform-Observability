# Data Platform Observability 

This project is an **Airflow Observability Platform** designed to monitor, analyze, and improve the reliability of data pipelines. Instead of processing business data, the system tracks **Airflow metadata itself** (DAG runs, task executions, failures, retries, and performance metrics) and transforms it into analytics-ready datasets.




##  Features

* Track Airflow DAG and task execution metadata
* Analyze pipeline health, failures, and performance trends
* Monitor data quality metrics
* Provide dashboards for observability and decision-making
* Simulate real production scenarios (success, failure, slow tasks)



## Data Pipeline Workflow

1. Airflow executes DAGs and generates metadata automatically
2. Python scripts extract metadata from Airflow DB
3. Raw metadata is loaded into an observability database
4. dbt transforms raw data into analytics models
5. Dashboards visualize pipeline health and trends
