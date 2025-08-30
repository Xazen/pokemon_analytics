#!/usr/bin/env python3
"""
Pokemon Showdown Usage Statistics Scraper

Scrapes competitive Pokemon usage data from Pokemon Showdown.
This is Phase 4 of the Pokemon Analytics project.

Data Sources:
- https://www.smogon.com/stats/ - Official usage statistics
- Monthly usage data by format (OU, UU, RU, etc.)
- Team composition and move usage data

Usage:
    python showdown_scraper.py --format OU --month 2025-08
"""

import requests
import pandas as pd
import psycopg2
import json
import time
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import argparse
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ShowdownScraper:
    """Scrapes Pokemon Showdown usage statistics from Smogon"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.base_url = "https://www.smogon.com/stats/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Pokemon Analytics Research Bot 1.0 (Educational Use)'
        })
        self.connection = None
        
        # Common competitive formats
        self.formats = [
            'gen9ou',      # Generation 9 OverUsed
            'gen9uu',      # Generation 9 UnderUsed  
            'gen9ru',      # Generation 9 RarelyUsed
            'gen9nu',      # Generation 9 NeverUsed
            'gen9vgc2024', # VGC 2024 format
        ]
    
    def connect_db(self) -> psycopg2.extensions.connection:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            logger.info("Connected to PostgreSQL database")
            return self.connection
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def close_db(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def get_available_months(self) -> List[str]:
        """Get list of available stat months from Smogon"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all month directories (format: YYYY-MM)
            months = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Match pattern like "2024-08/"
                if re.match(r'^\d{4}-\d{2}/$', href):
                    months.append(href.rstrip('/'))
            
            months.sort(reverse=True)  # Most recent first
            logger.info(f"Found {len(months)} available months: {months[:5]}...")
            return months
            
        except Exception as e:
            logger.error(f"Failed to get available months: {e}")
            return []
    
    def get_format_usage_data(self, month: str, format_name: str) -> Optional[Dict]:
        """Download usage data for a specific format and month"""
        try:
            # Construct URL for usage stats
            # Example: https://www.smogon.com/stats/2024-08/gen9ou-1695.txt
            usage_url = f"{self.base_url}{month}/{format_name}-1695.txt"
            
            logger.info(f"Fetching usage data: {usage_url}")
            response = self.session.get(usage_url, timeout=15)
            
            if response.status_code == 404:
                logger.warning(f"No data found for {format_name} in {month}")
                return None
                
            response.raise_for_status()
            
            # Parse usage statistics text file
            usage_data = self._parse_usage_text(response.text, month, format_name)
            
            logger.info(f"Successfully parsed {len(usage_data.get('pokemon', []))} Pokemon for {format_name}")
            return usage_data
            
        except Exception as e:
            logger.error(f"Failed to get usage data for {format_name} {month}: {e}")
            return None
    
    def _parse_usage_text(self, text_content: str, month: str, format_name: str) -> Dict:
        """Parse Smogon usage statistics text format"""
        pokemon_usage = []
        
        lines = text_content.strip().split('\n')
        
        # Find the usage section (after "+" divider and rank header)
        usage_started = False
        total_battles = 0
        
        for line in lines:
            line = line.strip()
            
            # Extract total battles
            if "Total battles:" in line:
                try:
                    total_battles = int(re.search(r'(\d+)', line).group(1))
                except:
                    pass
            
            # Start parsing after the rank header
            if line.startswith("| Rank | Pokemon"):
                usage_started = True
                continue
                
            if usage_started and line.startswith("|") and not line.startswith("| Rank"):
                # Parse line format: | Rank | Pokemon Name | Usage% | Raw | Raw%
                parts = [p.strip() for p in line.split('|')]
                
                if len(parts) >= 6:
                    try:
                        rank = int(parts[1])
                        pokemon_name = parts[2]
                        usage_percent = float(parts[3].rstrip('%'))
                        raw_count = int(parts[4])
                        raw_percent = float(parts[5].rstrip('%'))
                        
                        pokemon_usage.append({
                            'rank': rank,
                            'pokemon_name': pokemon_name.lower(),
                            'usage_percentage': usage_percent,
                            'raw_count': raw_count,
                            'raw_percentage': raw_percent,
                            'format': format_name,
                            'month_year': month,
                            'total_battles': total_battles
                        })
                        
                    except (ValueError, IndexError) as e:
                        logger.debug(f"Skipping malformed line: {line} - {e}")
                        continue
        
        return {
            'pokemon': pokemon_usage,
            'format': format_name,
            'month': month,
            'total_battles': total_battles,
            'scraped_at': datetime.now()
        }
    
    def store_usage_data(self, usage_data: Dict) -> bool:
        """Store usage data in PostgreSQL"""
        if not usage_data or not usage_data.get('pokemon'):
            logger.warning("No usage data to store")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Insert usage statistics
            insert_sql = """
                INSERT INTO raw.usage_stats 
                (pokemon_name, format, usage_percentage, rank_position, month_year, raw_count, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pokemon_name, format, month_year) 
                DO UPDATE SET
                    usage_percentage = EXCLUDED.usage_percentage,
                    rank_position = EXCLUDED.rank_position,
                    raw_count = EXCLUDED.raw_count,
                    created_at = EXCLUDED.created_at
            """
            
            records_inserted = 0
            for pokemon in usage_data['pokemon']:
                cursor.execute(insert_sql, (
                    pokemon['pokemon_name'],
                    pokemon['format'],
                    pokemon['usage_percentage'],
                    pokemon['rank'],
                    pokemon['month_year'],
                    pokemon['raw_count'],
                    datetime.now()
                ))
                records_inserted += 1
            
            # Commit transaction
            self.connection.commit()
            cursor.close()
            
            logger.info(f"✅ Successfully stored {records_inserted} usage records for {usage_data['format']} {usage_data['month']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store usage data: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def scrape_format_data(self, format_name: str, months: List[str], delay: float = 1.0) -> Tuple[int, int]:
        """Scrape usage data for a specific format across multiple months"""
        successful = 0
        total = 0
        
        logger.info(f"Starting scrape for format: {format_name}")
        
        for month in months:
            total += 1
            
            try:
                # Get usage data
                usage_data = self.get_format_usage_data(month, format_name)
                
                if usage_data and self.store_usage_data(usage_data):
                    successful += 1
                    logger.info(f"✅ {format_name} {month}: {len(usage_data['pokemon'])} Pokemon")
                else:
                    logger.warning(f"⚠️  {format_name} {month}: No data or storage failed")
                
                # Rate limiting
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"❌ {format_name} {month}: {e}")
        
        logger.info(f"Format {format_name} complete: {successful}/{total} months successful")
        return successful, total
    
    def get_scraping_stats(self) -> Dict[str, int]:
        """Get current scraping statistics from database"""
        try:
            cursor = self.connection.cursor()
            
            # Get statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT pokemon_name) as unique_pokemon,
                    COUNT(DISTINCT format) as unique_formats,
                    COUNT(DISTINCT month_year) as unique_months
                FROM raw.usage_stats
            """)
            
            stats = cursor.fetchone()
            cursor.close()
            
            return {
                'total_records': stats[0],
                'unique_pokemon': stats[1],
                'unique_formats': stats[2],
                'unique_months': stats[3]
            }
            
        except Exception as e:
            logger.error(f"Failed to get scraping stats: {e}")
            return {}

def main():
    """Main scraping function"""
    parser = argparse.ArgumentParser(description='Pokemon Showdown Usage Scraper')
    parser.add_argument('--format', type=str, default='gen9ou',
                       help='Pokemon format to scrape (default: gen9ou)')
    parser.add_argument('--months', type=int, default=3,
                       help='Number of recent months to scrape (default: 3)')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between requests in seconds (default: 1.0)')
    parser.add_argument('--all-formats', action='store_true',
                       help='Scrape all major competitive formats')
    
    args = parser.parse_args()
    
    # Import Docker utilities for environment-aware configuration
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))
    from utils.docker_utils import get_database_config, log_environment_info
    
    # Environment-aware database configuration
    log_environment_info()
    db_config = get_database_config()
    
    # Initialize scraper
    scraper = ShowdownScraper(db_config)
    
    try:
        # Connect to database
        scraper.connect_db()
        
        # Get initial stats
        initial_stats = scraper.get_scraping_stats()
        logger.info(f"Initial scraping stats: {initial_stats}")
        
        # Get available months
        available_months = scraper.get_available_months()
        if not available_months:
            raise Exception("No available months found")
        
        # Limit to recent months
        target_months = available_months[:args.months]
        logger.info(f"Target months: {target_months}")
        
        # Determine formats to scrape
        if args.all_formats:
            formats = scraper.formats
        else:
            formats = [args.format]
        
        # Scrape data
        total_successful = 0
        total_attempts = 0
        
        start_time = datetime.now()
        
        for format_name in formats:
            successful, attempts = scraper.scrape_format_data(
                format_name, target_months, args.delay
            )
            total_successful += successful
            total_attempts += attempts
        
        end_time = datetime.now()
        
        # Get final stats
        final_stats = scraper.get_scraping_stats()
        
        # Report results
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*60)
        logger.info("🎯 SCRAPING SUMMARY")
        logger.info("="*60)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Success rate: {total_successful}/{total_attempts} ({(total_successful/total_attempts)*100:.1f}%)")
        logger.info(f"Final stats: {final_stats}")
        logger.info(f"New records: {final_stats.get('total_records', 0) - initial_stats.get('total_records', 0)}")
        
        # Phase 4 completion check
        if total_successful > 0 and final_stats.get('total_records', 0) > 100:
            logger.info("🎉 Phase 4 COMPLETED: Successfully scraped competitive usage data!")
        else:
            logger.warning("⚠️  Phase 4 INCOMPLETE: Insufficient data collected")
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        return 1
    
    finally:
        scraper.close_db()
    
    return 0

if __name__ == "__main__":
    exit(main())