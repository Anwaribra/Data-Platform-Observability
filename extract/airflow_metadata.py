import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
from airflow.models import DagRun, TaskInstance
from airflow import settings

logger = logging.getLogger(__name__)

class AirflowMetadataExtractor:
    def __init__(self, observability_conn_id: str = 'observability_postgres'):
        self.observability_conn_id = observability_conn_id
        self.observability_engine = None
        
    def _get_observability_connection(self):
        """Get connection to observability PostgreSQL database."""
        if self.observability_engine is None:
            conn = BaseHook.get_connection(self.observability_conn_id)
            connection_string = (
                f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            )
            self.observability_engine = create_engine(connection_string)
        return self.observability_engine
    
    def extract_dag_runs(self, start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None) -> pd.DataFrame:
        logger.info("Extracting dag_run metadata from Airflow database")
        
        session = settings.Session()
        try:
            query = session.query(DagRun)
            
            if start_date:
                query = query.filter(DagRun.execution_date >= start_date)
            if end_date:
                query = query.filter(DagRun.execution_date <= end_date)
            
            dag_runs = query.all()
            
            dag_run_data = []
            for dr in dag_runs:
                dag_run_data.append({
                    'dag_id': dr.dag_id,
                    'run_id': dr.run_id,
                    'execution_date': dr.execution_date,
                    'state': dr.state,
                    'run_type': dr.run_type,
                    'conf': str(dr.conf) if dr.conf else None,
                    'start_date': dr.start_date,
                    'end_date': dr.end_date,
                    'data_interval_start': dr.data_interval_start,
                    'data_interval_end': dr.data_interval_end,
                    'last_scheduling_decision': dr.last_scheduling_decision,
                    'dag_hash': dr.dag_hash,
                    'creating_job_id': dr.creating_job_id,
                    'external_trigger': dr.external_trigger,
                    'run_note': dr.run_note,
                    'log_template_id': dr.log_template_id,
                    'extracted_at': datetime.utcnow()
                })
            
            df = pd.DataFrame(dag_run_data)
            logger.info(f"Extracted {len(df)} dag_run records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting dag_run metadata: {str(e)}")
            raise
        finally:
            session.close()
    
    def extract_task_instances(self, start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None) -> pd.DataFrame:
        logger.info("Extracting task_instance metadata from Airflow database")
        
        session = settings.Session()
        try:
            query = session.query(TaskInstance)
            
            if start_date:
                query = query.filter(TaskInstance.execution_date >= start_date)
            if end_date:
                query = query.filter(TaskInstance.execution_date <= end_date)
            
            task_instances = query.all()
            
            task_instance_data = []
            for ti in task_instances:
                task_instance_data.append({
                    'task_id': ti.task_id,
                    'dag_id': ti.dag_id,
                    'run_id': ti.run_id,
                    'execution_date': ti.execution_date,
                    'start_date': ti.start_date,
                    'end_date': ti.end_date,
                    'duration': ti.duration,
                    'state': ti.state,
                    'try_number': ti.try_number,
                    'max_tries': ti.max_tries,
                    'hostname': ti.hostname,
                    'unixname': ti.unixname,
                    'job_id': ti.job_id,
                    'pool': ti.pool,
                    'pool_slots': ti.pool_slots,
                    'queue': ti.queue,
                    'priority_weight': ti.priority_weight,
                    'operator': ti.operator,
                    'queued_dttm': ti.queued_dttm,
                    'queued_by_job_id': ti.queued_by_job_id,
                    'pid': ti.pid,
                    'updated_at': ti.updated_at,
                    'rendered_fields': str(ti.rendered_fields) if ti.rendered_fields else None,
                    'extracted_at': datetime.utcnow()
                })
            
            df = pd.DataFrame(task_instance_data)
            logger.info(f"Extracted {len(df)} task_instance records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting task_instance metadata: {str(e)}")
            raise
        finally:
            session.close()
    
    def load_to_observability_db(self, df: pd.DataFrame, table_name: str, 
                                 if_exists: str = 'append') -> None:
        if df.empty:
            logger.warning(f"DataFrame is empty, skipping load to {table_name}")
            return
        
        logger.info(f"Loading {len(df)} records to {table_name} table")
        
        engine = self._get_observability_connection()
        
        try:
            # Ensure table exists with proper schema
            self._create_table_if_not_exists(table_name, df, engine)
            
            # Load data
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Successfully loaded {len(df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            raise
    
    def _create_table_if_not_exists(self, table_name: str, df: pd.DataFrame, engine) -> None:
       
        try:
          
            with engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables "
                    f"WHERE table_name = '{table_name}')"
                ))
                table_exists = result.scalar()
                
                if not table_exists:
                    logger.info(f"Creating table {table_name}")
                   
                    df.head(0).to_sql(name=table_name, con=engine, if_exists='fail', index=False)
                    logger.info(f"Table {table_name} created successfully")
        except Exception as e:
            logger.warning(f"Could not create table {table_name}: {str(e)}")
           
    def extract_and_load(self, start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None) -> Dict[str, int]:
        results = {}
        
        try:
            dag_runs_df = self.extract_dag_runs(start_date, end_date)
            self.load_to_observability_db(dag_runs_df, 'dag_runs')
            results['dag_runs_count'] = len(dag_runs_df)
            task_instances_df = self.extract_task_instances(start_date, end_date)
            self.load_to_observability_db(task_instances_df, 'task_instances')
            results['task_instances_count'] = len(task_instances_df)
            logger.info(f"Extraction and load completed: {results}")
            return results
        except Exception as e:
            logger.error(f"Error in extract_and_load: {str(e)}")
            raise

