#!/usr/bin/env python3
"""
Competitive Pokemon Scraping DAG

Orchestrates weekly scraping of competitive Pokemon usage statistics.
This completes Phase 4 of the Pokemon Analytics project.

DAG Schedule: Weekly on Sundays at 3 AM UTC
Tasks: Scrape Usage Stats -> Validate Data -> Generate Insights

Author: Pokemon Analytics Team
"""

from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Add src directory to Python path
sys.path.append('/opt/airflow/src')

# Default DAG arguments
default_args = {
    'owner': 'pokemon_analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'max_active_runs': 1,
}

# DAG definition
dag = DAG(
    'competitive_pokemon_scraping',
    default_args=default_args,
    description='Weekly competitive Pokemon usage statistics scraping',
    schedule_interval='0 3 * * 0',  # Weekly on Sunday at 3 AM UTC
    catchup=False,
    tags=['pokemon', 'scraping', 'competitive', 'weekly'],
)

def scrape_competitive_data(**context):
    """Scrape competitive usage data from Pokemon Showdown"""
    import subprocess
    import logging
    
    logging.info("Starting competitive Pokemon scraping...")
    
    try:
        # Install required packages in Airflow container
        subprocess.run([
            'pip', 'install', 'beautifulsoup4', 'pandas'
        ], check=True, cwd='/opt/airflow')
        
        # Run scraper for OU format (most popular)
        result = subprocess.run([
            'python3', '/opt/airflow/src/scrapers/showdown_scraper.py',
            '--format', 'gen9ou',
            '--months', '2',
            '--delay', '2.0'
        ], capture_output=True, text=True, cwd='/opt/airflow')
        
        if result.returncode == 0:
            logging.info("✅ Competitive scraping completed successfully")
            logging.info(f"Scraper output: {result.stdout}")
            
            # Extract statistics from output
            lines = result.stdout.split('\n')
            stats = {}
            for line in lines:
                if 'Success rate:' in line:
                    stats['success_rate'] = line.split(':')[1].strip()
                elif 'New records:' in line:
                    stats['new_records'] = line.split(':')[1].strip()
                elif 'Final stats:' in line:
                    stats['final_stats'] = line.split(':', 1)[1].strip()
            
            return {"status": "success", "format": "gen9ou", "stats": stats}
        else:
            logging.error(f"❌ Competitive scraping failed: {result.stderr}")
            raise Exception(f"Scraping failed: {result.stderr}")
            
    except Exception as e:
        logging.error(f"Competitive scraping error: {e}")
        raise

def validate_competitive_data(**context):
    """Validate scraped competitive data"""
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
        
        # Check competitive data completeness
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT pokemon_name) as unique_pokemon,
                COUNT(DISTINCT format) as unique_formats,
                MAX(usage_percentage) as max_usage,
                MIN(usage_percentage) as min_usage,
                AVG(usage_percentage) as avg_usage
            FROM raw.usage_stats
        """)
        
        stats = cursor.fetchone()
        
        # Validation checks
        total_records = stats[0]
        unique_pokemon = stats[1]
        max_usage = stats[3]
        min_usage = stats[4]
        avg_usage = stats[5]
        
        if total_records < 100:
            raise ValueError(f"Insufficient competitive records: {total_records}")
        
        if unique_pokemon < 50:
            raise ValueError(f"Too few unique Pokemon: {unique_pokemon}")
            
        if max_usage > 100 or max_usage <= 0:
            raise ValueError(f"Invalid max usage: {max_usage}%")
            
        if min_usage < 0:
            raise ValueError(f"Invalid min usage: {min_usage}%")
        
        # Check for recent data
        cursor.execute("""
            SELECT COUNT(*) FROM raw.usage_stats 
            WHERE created_at >= NOW() - INTERVAL '1 day'
        """)
        recent_count = cursor.fetchone()[0]
        
        logging.info(f"✅ Competitive data validation passed")
        logging.info(f"Records: {total_records}, Pokemon: {unique_pokemon}")
        logging.info(f"Usage range: {min_usage:.1f}% - {max_usage:.1f}% (avg: {avg_usage:.1f}%)")
        logging.info(f"Recent records: {recent_count}")
        
        cursor.close()
        connection.close()
        
        return {
            "total_records": total_records,
            "unique_pokemon": unique_pokemon,
            "usage_range": f"{min_usage:.1f}%-{max_usage:.1f}%",
            "recent_count": recent_count,
            "status": "validated"
        }
        
    except Exception as e:
        logging.error(f"Competitive data validation failed: {e}")
        raise

def generate_competitive_insights(**context):
    """Generate competitive meta insights"""
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
        
        # Top 10 Pokemon by usage
        cursor.execute("""
            SELECT pokemon_name, format, usage_percentage, rank_position
            FROM raw.usage_stats 
            WHERE rank_position <= 10
            ORDER BY usage_percentage DESC
            LIMIT 10
        """)
        
        top_pokemon = cursor.fetchall()
        
        # Format distribution
        cursor.execute("""
            SELECT format, COUNT(*) as pokemon_count, AVG(usage_percentage) as avg_usage
            FROM raw.usage_stats
            GROUP BY format
            ORDER BY pokemon_count DESC
        """)
        
        format_stats = cursor.fetchall()
        
        # Meta diversity (Pokemon with >10% usage)
        cursor.execute("""
            SELECT COUNT(*) as high_usage_count
            FROM raw.usage_stats 
            WHERE usage_percentage >= 10.0
        """)
        
        high_usage_count = cursor.fetchone()[0]
        
        insights = {
            "analysis_date": context['ds'],
            "top_pokemon": [
                {
                    "name": row[0],
                    "format": row[1], 
                    "usage": float(row[2]),
                    "rank": row[3]
                } for row in top_pokemon
            ],
            "format_distribution": [
                {
                    "format": row[0],
                    "pokemon_count": row[1],
                    "avg_usage": float(row[2])
                } for row in format_stats
            ],
            "high_usage_pokemon": high_usage_count,
            "meta_leader": top_pokemon[0][0] if top_pokemon else "Unknown"
        }
        
        logging.info(f"📊 Competitive Insights Generated")
        logging.info(f"Meta leader: {insights['meta_leader']}")
        logging.info(f"High usage Pokemon (>10%): {high_usage_count}")
        logging.info(f"Formats analyzed: {len(format_stats)}")
        
        # Store in XCom
        context['task_instance'].xcom_push(key='competitive_insights', value=insights)
        
        cursor.close()
        connection.close()
        
        return insights
        
    except Exception as e:
        logging.error(f"Competitive insights generation failed: {e}")
        raise

# Task definitions
with dag:
    
    start_competitive = DummyOperator(
        task_id='start_competitive_pipeline',
        dag=dag
    )
    
    # Data collection
    scrape_data = PythonOperator(
        task_id='scrape_competitive_data',
        python_callable=scrape_competitive_data,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
    )
    
    # Data validation
    validate_data = PythonOperator(
        task_id='validate_competitive_data',
        python_callable=validate_competitive_data,
        dag=dag,
    )
    
    # Generate insights
    generate_insights = PythonOperator(
        task_id='generate_competitive_insights',
        python_callable=generate_competitive_insights,
        dag=dag,
    )
    
    # Competitive report
    competitive_report = BashOperator(
        task_id='generate_competitive_report',
        bash_command='''
        echo "🏆 COMPETITIVE POKEMON REPORT"
        echo "================================"
        python3 -c "
import psycopg2
from datetime import datetime

conn = psycopg2.connect(host='postgres', database='pokemon_analytics', 
                       user='pokemon_user', password='pokemon_pass')
cur = conn.cursor()

print('📊 CURRENT COMPETITIVE META:')
cur.execute(\"\"\"
    SELECT pokemon_name, format, usage_percentage, rank_position 
    FROM raw.usage_stats 
    WHERE rank_position <= 5 
    ORDER BY usage_percentage DESC
\"\"\")

for name, format, usage, rank in cur.fetchall():
    print(f'  #{rank}: {name.title()} ({format}) - {usage:.1f}%')

print('\n🎯 META INSIGHTS:')
cur.execute('SELECT COUNT(DISTINCT pokemon_name) FROM raw.usage_stats')
total_pokemon = cur.fetchone()[0]

cur.execute('SELECT COUNT(*) FROM raw.usage_stats WHERE usage_percentage >= 10')
viable_pokemon = cur.fetchone()[0]

print(f'  Total Pokemon tracked: {total_pokemon}')
print(f'  Highly viable (>10%): {viable_pokemon}')
print(f'  Meta diversity: {(viable_pokemon/total_pokemon)*100:.1f}%')

conn.close()
print('\n✅ Competitive report complete')
        "
        ''',
        dag=dag,
    )
    
    end_competitive = DummyOperator(
        task_id='competitive_pipeline_complete',
        dag=dag
    )
    
    # Task dependencies
    start_competitive >> scrape_data >> validate_data >> generate_insights >> competitive_report >> end_competitive

# DAG documentation  
dag.doc_md = """
# Competitive Pokemon Scraping Pipeline

## Overview
Weekly automated scraping of competitive Pokemon usage statistics from Pokemon Showdown via Smogon.

## Data Source
- **Smogon Usage Stats**: https://www.smogon.com/stats/
- **Primary Format**: Generation 9 OverUsed (gen9ou)
- **Update Frequency**: Weekly (Sundays at 3:00 AM UTC)
- **Historical Depth**: 2 months of data per run

## Pipeline Flow
1. **Scrape Data**: Collect latest usage statistics from Pokemon Showdown
2. **Validate Data**: Quality checks on scraped competitive data
3. **Generate Insights**: Meta analysis and top Pokemon identification
4. **Report Generation**: Competitive summary with key metrics

## Key Metrics
- **Usage Percentage**: How often Pokemon appear in competitive teams
- **Rank Position**: Pokemon ranking by usage frequency  
- **Meta Diversity**: Number of viable Pokemon (>10% usage)
- **Format Coverage**: Multiple competitive formats tracked

## Data Quality Gates
- Minimum 100 competitive records per run
- Usage percentages validated (0-100% range)
- Recent data freshness checks
- Automatic retry on failures (2 attempts)

## Outputs
- `raw.usage_stats`: Individual Pokemon competitive usage records
- Competitive insights stored in XCom for downstream analysis
- Formatted competitive report with meta leaders and trends
"""