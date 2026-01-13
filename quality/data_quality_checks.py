import logging
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)
class DataQualityChecker:
    def __init__(self, observability_conn_id: str = 'observability_postgres'):
        self.observability_conn_id = observability_conn_id
        self.observability_engine = None
        self.check_results = []
        
    def _get_observability_connection(self):
        if self.observability_engine is None:
            conn = BaseHook.get_connection(self.observability_conn_id)
            connection_string = (
                f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            )
            self.observability_engine = create_engine(connection_string)
        return self.observability_engine
    
    def check_table_exists(self, table_name: str) -> bool:
        try:
            engine = self._get_observability_connection()
            with engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables "
                    f"WHERE table_name = '{table_name}')"
                ))
                exists = result.scalar()
                logger.info(f"Table {table_name} exists: {exists}")
                return exists
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {str(e)}")
            return False
    
    def check_row_count(self, table_name: str, min_rows: int = 0) -> Dict[str, any]:
        check_name = f"row_count_{table_name}"
        try:
            engine = self._get_observability_connection()
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                
                passed = row_count >= min_rows
                result_dict = {
                    'check_name': check_name,
                    'table_name': table_name,
                    'passed': passed,
                    'row_count': row_count,
                    'min_expected': min_rows,
                    'message': f"Row count check: {row_count} >= {min_rows}"
                }
                
                if passed:
                    logger.info(f" {check_name}: PASSED - {row_count} rows")
                else:
                    logger.warning(f"✗ {check_name}: FAILED - {row_count} rows (expected >= {min_rows})")
                
                self.check_results.append(result_dict)
                return result_dict
                
        except Exception as e:
            error_msg = f"Error checking row count for {table_name}: {str(e)}"
            logger.error(error_msg)
            result_dict = {
                'check_name': check_name,
                'table_name': table_name,
                'passed': False,
                'error': str(e),
                'message': error_msg
            }
            self.check_results.append(result_dict)
            return result_dict
    
    def check_null_values(self, table_name: str, column_name: str, 
                         max_null_percentage: float = 0.0) -> Dict[str, any]:

        check_name = f"null_check_{table_name}_{column_name}"
        try:
            engine = self._get_observability_connection()
            with engine.connect() as conn:
                # Get total count
                total_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_count = total_result.scalar()
                
                if total_count == 0:
                    result_dict = {
                        'check_name': check_name,
                        'table_name': table_name,
                        'column_name': column_name,
                        'passed': False,
                        'null_count': 0,
                        'total_count': 0,
                        'null_percentage': 0.0,
                        'message': 'Table is empty'
                    }
                    self.check_results.append(result_dict)
                    return result_dict

                null_result = conn.execute(text(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
                ))
                null_count = null_result.scalar()
                null_percentage = null_count / total_count if total_count > 0 else 0.0
                
                passed = null_percentage <= max_null_percentage
                result_dict = {
                    'check_name': check_name,
                    'table_name': table_name,
                    'column_name': column_name,
                    'passed': passed,
                    'null_count': null_count,
                    'total_count': total_count,
                    'null_percentage': null_percentage,
                    'max_allowed': max_null_percentage,
                    'message': f"Null check: {null_percentage:.2%} null values (max allowed: {max_null_percentage:.2%})"
                }
                
                if passed:
                    logger.info(f"✓ {check_name}: PASSED - {null_percentage:.2%} null values")
                else:
                    logger.warning(f"✗ {check_name}: FAILED - {null_percentage:.2%} null values (max: {max_null_percentage:.2%})")
                
                self.check_results.append(result_dict)
                return result_dict
                
        except Exception as e:
            error_msg = f"Error checking null values for {table_name}.{column_name}: {str(e)}"
            logger.error(error_msg)
            result_dict = {
                'check_name': check_name,
                'table_name': table_name,
                'column_name': column_name,
                'passed': False,
                'error': str(e),
                'message': error_msg
            }
            self.check_results.append(result_dict)
            return result_dict
    
    def check_data_freshness(self, table_name: str, timestamp_column: str = 'extracted_at',
                           max_age_hours: int = 25) -> Dict[str, any]:

        check_name = f"freshness_check_{table_name}"
        try:
            engine = self._get_observability_connection()
            with engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT MAX({timestamp_column}) FROM {table_name}"
                ))
                max_timestamp = result.scalar()
                
                if max_timestamp is None:
                    result_dict = {
                        'check_name': check_name,
                        'table_name': table_name,
                        'passed': False,
                        'message': 'No timestamp found in table'
                    }
                    self.check_results.append(result_dict)
                    return result_dict
                
            
                from datetime import datetime, timezone
                if isinstance(max_timestamp, str):
                    max_timestamp = pd.to_datetime(max_timestamp)
                
                age_hours = (datetime.now(timezone.utc) - max_timestamp).total_seconds() / 3600
                passed = age_hours <= max_age_hours
                
                result_dict = {
                    'check_name': check_name,
                    'table_name': table_name,
                    'passed': passed,
                    'max_timestamp': str(max_timestamp),
                    'age_hours': age_hours,
                    'max_allowed_hours': max_age_hours,
                    'message': f"Data freshness: {age_hours:.2f} hours old (max allowed: {max_age_hours} hours)"
                }
                if passed:
                    logger.info(f"✓ {check_name}: PASSED - Data is {age_hours:.2f} hours old")
                else:
                    logger.warning(f"✗ {check_name}: FAILED - Data is {age_hours:.2f} hours old (max: {max_age_hours} hours)")
                
                self.check_results.append(result_dict)
                return result_dict
                
        except Exception as e:
            error_msg = f"Error checking data freshness for {table_name}: {str(e)}"
            logger.error(error_msg)
            result_dict = {
                'check_name': check_name,
                'table_name': table_name,
                'passed': False,
                'error': str(e),
                'message': error_msg
            }
            self.check_results.append(result_dict)
            return result_dict
    
    def run_all_checks(self) -> Dict[str, any]:

        logger.info("Starting data quality checks")
    
        dag_runs_exists = self.check_table_exists('dag_runs')
        task_instances_exists = self.check_table_exists('task_instances')
        
        if not dag_runs_exists or not task_instances_exists:
            logger.error("Required tables do not exist")
            return {
                'all_passed': False,
                'message': 'Required tables missing',
                'checks': self.check_results
            }

        self.check_row_count('dag_runs', min_rows=0)
        self.check_null_values('dag_runs', 'dag_id', max_null_percentage=0.0)
        self.check_null_values('dag_runs', 'run_id', max_null_percentage=0.0)
        self.check_data_freshness('dag_runs', timestamp_column='extracted_at', max_age_hours=25)
        
        self.check_row_count('task_instances', min_rows=0)
        self.check_null_values('task_instances', 'task_id', max_null_percentage=0.0)
        self.check_null_values('task_instances', 'dag_id', max_null_percentage=0.0)
        self.check_data_freshness('task_instances', timestamp_column='extracted_at', max_age_hours=25)
        all_passed = all(check.get('passed', False) for check in self.check_results)
        
        passed_count = sum(1 for check in self.check_results if check.get('passed', False))
        total_count = len(self.check_results)
        
        logger.info(f"Data quality checks completed: {passed_count}/{total_count} passed")
        
        return {
            'all_passed': all_passed,
            'passed_count': passed_count,
            'total_count': total_count,
            'checks': self.check_results
        }

