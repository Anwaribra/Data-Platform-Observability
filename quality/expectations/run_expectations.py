import logging
from typing import Dict, Optional
from great_expectations import DataContext
from great_expectations.core.batch import RuntimeBatchRequest
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def run_dag_runs_expectations(
    observability_conn_id: str = 'observability_postgres',
    table_name: str = 'dag_runs',
    ge_context_root_dir: Optional[str] = None
) -> Dict:
    """
    Run Great Expectations checks on dag_runs table.
    
    Args:
        observability_conn_id: Airflow connection ID for observability database
        table_name: Name of the table to validate
        ge_context_root_dir: Optional path to Great Expectations context root
        
    Returns:
        Dictionary with validation results
    """
    try:
        
        conn = BaseHook.get_connection(observability_conn_id)
        connection_string = (
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )
        
        
        if ge_context_root_dir:
            context = DataContext(ge_context_root_dir)
        else:
            context = DataContext()
        
        batch_request = RuntimeBatchRequest(
            datasource_name="observability_postgres",
            data_connector_name="default_runtime_data_connector",
            data_asset_name=table_name,
            runtime_parameters={"query": f"SELECT * FROM {table_name}"},
            batch_identifiers={"default_identifier_name": "default_identifier"}
        )
        
        from quality.expectations.dag_runs_expectations import get_dag_runs_expectation_suite
        suite = get_dag_runs_expectation_suite()
        
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite.expectation_suite_name
        )
        
        checkpoint_result = validator.validate()
        
        logger.info(f"Great Expectations validation completed for {table_name}")
        logger.info(f"Success: {checkpoint_result.success}")
        
        return {
            "success": checkpoint_result.success,
            "statistics": checkpoint_result.statistics,
            "results": checkpoint_result.results
        }
        
    except Exception as e:
        logger.error(f"Error running Great Expectations checks: {str(e)}")
        raise


def run_task_instances_expectations(
    observability_conn_id: str = 'observability_postgres',
    table_name: str = 'task_instances',
    ge_context_root_dir: Optional[str] = None
) -> Dict:
    """
    Run Great Expectations checks on task_instances table.
    
    Args:
        observability_conn_id: Airflow connection ID for observability database
        table_name: Name of the table to validate
        ge_context_root_dir: Optional path to Great Expectations context root
        
    Returns:
        Dictionary with validation results
    """
    try:
        
        conn = BaseHook.get_connection(observability_conn_id)
        connection_string = (
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )
        if ge_context_root_dir:
            context = DataContext(ge_context_root_dir)
        else:
            context = DataContext()
        
        batch_request = RuntimeBatchRequest(
            datasource_name="observability_postgres",
            data_connector_name="default_runtime_data_connector",
            data_asset_name=table_name,
            runtime_parameters={"query": f"SELECT * FROM {table_name}"},
            batch_identifiers={"default_identifier_name": "default_identifier"}
        )

        from quality.expectations.task_instances_expectations import get_task_instances_expectation_suite
        suite = get_task_instances_expectation_suite()

        # Run validation
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite.expectation_suite_name
        )
        
        checkpoint_result = validator.validate()
        
        logger.info(f"Great Expectations validation completed for {table_name}")
        logger.info(f"Success: {checkpoint_result.success}")
        
        return {
            "success": checkpoint_result.success,
            "statistics": checkpoint_result.statistics,
            "results": checkpoint_result.results
        }
        
    except Exception as e:
        logger.error(f"Error running Great Expectations checks: {str(e)}")
        raise

