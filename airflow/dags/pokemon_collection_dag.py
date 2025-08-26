#!/usr/bin/env python3
"""
Pokemon Collection DAG

Orchestrates the Pokemon data collection and processing workflow.
This is Phase 3.5 of the Pokemon Analytics project.

DAG Schedule: Runs daily at 2 AM UTC
Tasks: Data Collection -> Data Validation -> Basic Processing

Author: Pokemon Analytics Team
"""

from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Add src directory to Python path for imports
sys.path.append('/opt/airflow/src')

# Default DAG arguments
default_args = {
    'owner': 'pokemon_analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,  # Prevent overlapping runs
}

# DAG definition
dag = DAG(
    'pokemon_collection_pipeline',
    default_args=default_args,
    description='Daily Pokemon data collection and processing pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,  # Don't run for past dates
    tags=['pokemon', 'data-collection', 'etl'],
)

def check_database_connection():
    """Test database connectivity before starting pipeline"""
    import psycopg2
    
    db_config = {
        'host': 'postgres',
        'port': '5432',
        'database': 'pokemon_analytics',
        'user': 'pokemon_user',
        'password': 'pokemon_pass'
    }
    
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        connection.close()
        print("✅ Database connection successful")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise

def collect_pokemon_batch(**context):
    """Collect Pokemon data using our existing collector"""
    import subprocess
    import logging
    
    logging.info("Starting Pokemon data collection...")
    
    try:
        # Install required packages in the Airflow container
        subprocess.run([
            'pip', 'install', 'requests', 'psycopg2-binary'
        ], check=True, cwd='/opt/airflow')
        
        # Run the Pokemon collector
        result = subprocess.run([
            'python3', '/opt/airflow/src/collectors/pokemon_collector.py',
            '--limit', '10',  # Collect 10 Pokemon for daily updates
            '--delay', '0.2'  # Be respectful to the API
        ], capture_output=True, text=True, cwd='/opt/airflow')
        
        if result.returncode == 0:
            logging.info("✅ Pokemon collection completed successfully")
            logging.info(f"Output: {result.stdout}")
            return {"status": "success", "message": "Collection completed"}
        else:
            logging.error(f"❌ Pokemon collection failed: {result.stderr}")
            raise Exception(f"Collection failed: {result.stderr}")
            
    except Exception as e:
        logging.error(f"Pokemon collection error: {e}")
        raise

def validate_collected_data(**context):
    """Validate the collected Pokemon data"""
    import psycopg2
    import logging
    
    db_config = {
        'host': 'postgres',
        'port': '5432',
        'database': 'pokemon_analytics',
        'user': 'pokemon_user',
        'password': 'pokemon_pass'
    }
    
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Check Pokemon count
        cursor.execute("SELECT COUNT(*) FROM raw.pokemon")
        pokemon_count = cursor.fetchone()[0]
        
        # Check data completeness
        cursor.execute("""
            SELECT 
                COUNT(*) as total_pokemon,
                COUNT(CASE WHEN height IS NOT NULL THEN 1 END) as with_height,
                COUNT(CASE WHEN weight IS NOT NULL THEN 1 END) as with_weight
            FROM raw.pokemon
        """)
        
        stats = cursor.fetchone()
        
        # Data quality checks
        if pokemon_count == 0:
            raise ValueError("No Pokemon found in database")
        
        completeness_ratio = min(stats[1], stats[2]) / stats[0]
        if completeness_ratio < 0.9:  # 90% completeness threshold
            raise ValueError(f"Data completeness too low: {completeness_ratio:.2%}")
        
        logging.info(f"✅ Data validation passed: {pokemon_count} Pokemon with {completeness_ratio:.1%} completeness")
        
        cursor.close()
        connection.close()
        
        return {
            "pokemon_count": pokemon_count,
            "completeness_ratio": completeness_ratio,
            "status": "validated"
        }
        
    except Exception as e:
        logging.error(f"Data validation failed: {e}")
        raise

def generate_daily_summary(**context):
    """Generate daily summary statistics"""
    import psycopg2
    import json
    import logging
    
    db_config = {
        'host': 'postgres',
        'port': '5432',
        'database': 'pokemon_analytics',
        'user': 'pokemon_user',
        'password': 'pokemon_pass'
    }
    
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_pokemon,
                ROUND(AVG(height), 2) as avg_height,
                ROUND(AVG(weight), 2) as avg_weight,
                COUNT(DISTINCT pt.type_name) as unique_types
            FROM raw.pokemon p
            LEFT JOIN raw.pokemon_types pt ON p.id = pt.pokemon_id
        """)
        
        stats = cursor.fetchone()
        
        summary = {
            "date": context['ds'],  # Execution date
            "total_pokemon": stats[0],
            "avg_height": float(stats[1]) if stats[1] else None,
            "avg_weight": float(stats[2]) if stats[2] else None,
            "unique_types": stats[3],
            "pipeline_status": "success"
        }
        
        logging.info(f"📊 Daily Summary: {json.dumps(summary, indent=2)}")
        
        # Store summary in XCom for downstream tasks
        context['task_instance'].xcom_push(key='daily_summary', value=summary)
        
        cursor.close()
        connection.close()
        
        return summary
        
    except Exception as e:
        logging.error(f"Summary generation failed: {e}")
        raise

# Task Groups for better organization
with dag:
    
    # Start task
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )
    
    # Pre-flight checks
    with TaskGroup('preflight_checks', dag=dag) as preflight_group:
        
        check_db = PythonOperator(
            task_id='check_database_connection',
            python_callable=check_database_connection,
            dag=dag
        )
        
        check_disk_space = BashOperator(
            task_id='check_disk_space',
            bash_command='df -h /opt/airflow && echo "✅ Disk space check completed"',
            dag=dag
        )
    
    # Data collection group
    with TaskGroup('data_collection', dag=dag) as collection_group:
        
        collect_pokemon = PythonOperator(
            task_id='collect_pokemon_data',
            python_callable=collect_pokemon_batch,
            dag=dag,
            pool='pokemon_api_pool',  # Limit concurrent API calls
            execution_timeout=timedelta(minutes=10),
        )
        
        validate_data = PythonOperator(
            task_id='validate_collected_data',
            python_callable=validate_collected_data,
            dag=dag,
        )
    
    # Data processing group
    with TaskGroup('data_processing', dag=dag) as processing_group:
        
        # Simple data quality check
        data_quality_check = BashOperator(
            task_id='run_data_quality_checks',
            bash_command='''
            echo "Running data quality checks..."
            python3 -c "
import psycopg2
conn = psycopg2.connect(host='postgres', database='pokemon_analytics', 
                       user='pokemon_user', password='pokemon_pass')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM raw.pokemon WHERE name IS NULL OR name = \'\'')
null_names = cur.fetchone()[0]
if null_names > 0:
    raise ValueError(f'Found {null_names} Pokemon with null/empty names')
print('✅ Data quality checks passed')
conn.close()
            "
            ''',
            dag=dag,
        )
        
        # Generate summary
        daily_summary = PythonOperator(
            task_id='generate_daily_summary',
            python_callable=generate_daily_summary,
            dag=dag,
        )
    
    # End task
    pipeline_complete = DummyOperator(
        task_id='pipeline_complete',
        dag=dag
    )
    
    # Task dependencies
    start_pipeline >> preflight_group >> collection_group >> processing_group >> pipeline_complete
    
    # Within groups
    check_db >> collect_pokemon
    collect_pokemon >> validate_data
    validate_data >> data_quality_check >> daily_summary

# DAG documentation
dag.doc_md = """
# Pokemon Collection Pipeline

## Overview
This DAG orchestrates the daily Pokemon data collection and processing workflow.

## Tasks
1. **Preflight Checks**: Database connectivity and system health
2. **Data Collection**: Fetch Pokemon data from PokeAPI
3. **Data Validation**: Ensure data quality and completeness
4. **Data Processing**: Basic transformations and quality checks
5. **Summary Generation**: Create daily statistics and reports

## Schedule
- **Frequency**: Daily at 2:00 AM UTC
- **Catchup**: Disabled (won't run for historical dates)
- **Max Active Runs**: 1 (prevents overlapping executions)

## Monitoring
- Check Airflow UI for task status and logs
- Failed tasks will retry once after 5-minute delay
- Summary statistics available in XCom

## Dependencies
- PostgreSQL database must be accessible
- Pokemon collector script must be available in `/opt/airflow/src/`
- Required Python packages: requests, psycopg2-binary
"""