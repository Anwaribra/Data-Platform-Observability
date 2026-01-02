from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from extract.airflow_metadata import AirflowMetadataExtractor
from quality.data_quality_checks import DataQualityChecker

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['anwaribrrahim@gmail.com'],  
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'max_active_runs': 1,
}

# dag 
dag = DAG(
    'airflow_observability_pipeline',
    default_args=default_args,
    description='Extract Airflow metadata and load into observability database',
    schedule_interval='0 2 * * *',  
    start_date=days_ago(1),
    catchup=False,
    tags=['observability', 'metadata', 'data-quality'],
    doc_md="""
    # Airflow Observability Pipeline
    
    This DAG extracts metadata from Airflow's metadata database and loads it
    into an observability PostgreSQL database for monitoring and analysis.
    
    ## Tasks
    
    1. **extract_airflow_metadata**: Extracts dag_run and task_instance metadata
    2. **run_data_quality_checks**: Runs data quality checks on loaded data
    
    ## Configuration
    
    - Requires Airflow connection: `observability_postgres`
    - Connection should point to the observability PostgreSQL database
    - Tables created automatically: `dag_runs`, `task_instances`

    """,
)


def extract_metadata_task(**context):
    import logging
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    logger.info("Starting metadata extraction task")
    
    try:
        
        execution_date = context['execution_date']
        start_date = execution_date - timedelta(days=1)
        end_date = execution_date
        
        logger.info(f"Extracting metadata from {start_date} to {end_date}")
        
        extractor = AirflowMetadataExtractor(observability_conn_id='observability_postgres')
        
        results = extractor.extract_and_load(start_date=start_date, end_date=end_date)
        
        logger.info(f"Metadata extraction completed successfully: {results}")

        return results
        
    except Exception as e:
        logger.error(f"Error in metadata extraction: {str(e)}", exc_info=True)
        raise


def run_quality_checks_task(**context):
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data quality checks task")
    
    try:
        ti = context['ti']
        extraction_results = ti.xcom_pull(task_ids='extract_airflow_metadata')
        
        if extraction_results:
            logger.info(f"Previous extraction results: {extraction_results}")

        checker = DataQualityChecker(observability_conn_id='observability_postgres')

        check_results = checker.run_all_checks()
        
        logger.info(f"Data quality checks completed: {check_results['passed_count']}/{check_results['total_count']} passed")
        
        for check in check_results['checks']:
            status = "PASSED" if check.get('passed', False) else "FAILED"
            logger.info(f"  {check.get('check_name', 'unknown')}: {status} - {check.get('message', '')}")

        if not check_results['all_passed']:
            failed_checks = [c for c in check_results['checks'] if not c.get('passed', False)]
            error_msg = f"Data quality checks failed: {len(failed_checks)} check(s) failed"
            logger.error(error_msg)
            logger.warning("Continuing despite quality check failures (configured as warning)")
        
        return check_results
        
    except Exception as e:
        logger.error(f"Error in data quality checks: {str(e)}", exc_info=True)
        raise


# Task definitions
extract_metadata = PythonOperator(
    task_id='extract_airflow_metadata',
    python_callable=extract_metadata_task,
    provide_context=True,
    dag=dag,
    doc_md="""
    Extracts dag_run and task_instance metadata from Airflow's metadata database
    and loads it into the observability PostgreSQL database.
    """,
)

run_quality_checks = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_quality_checks_task,
    provide_context=True,
    dag=dag,
    doc_md="""
    Runs data quality checks on the loaded observability data including:
    - Row count validation
    - Null value checks for critical columns
    - Data freshness validation
    """,
)

extract_metadata >> run_quality_checks

