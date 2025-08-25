#!/usr/bin/env python3
"""
Pokemon API Data Collector

Collects basic Pokemon data from the PokeAPI and stores it in PostgreSQL.
This is Phase 2 of the Pokemon Analytics project.

Usage:
    python pokemon_collector.py --limit 151  # Collect first 151 Pokemon
    python pokemon_collector.py --all        # Collect all Pokemon
"""

import requests
import psycopg2
import json
import time
import logging
from typing import Dict, List, Optional, Tuple
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PokeAPICollector:
    """Collects Pokemon data from PokeAPI and stores in PostgreSQL"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.base_url = "https://pokeapi.co/api/v2"
        self.session = requests.Session()
        self.connection = None
        
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
    
    def get_pokemon_data(self, pokemon_id: int) -> Optional[Dict]:
        """Fetch Pokemon data from PokeAPI"""
        try:
            url = f"{self.base_url}/pokemon/{pokemon_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"Fetched data for Pokemon ID {pokemon_id}: {data['name']}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Pokemon {pokemon_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching Pokemon {pokemon_id}: {e}")
            return None
    
    def insert_pokemon_base(self, cursor, pokemon_data: Dict) -> bool:
        """Insert basic Pokemon information"""
        try:
            insert_sql = """
                INSERT INTO raw.pokemon (id, name, height, weight, base_experience, order_id, is_default)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    base_experience = EXCLUDED.base_experience,
                    order_id = EXCLUDED.order_id,
                    is_default = EXCLUDED.is_default
            """
            
            cursor.execute(insert_sql, (
                pokemon_data['id'],
                pokemon_data['name'],
                pokemon_data['height'],
                pokemon_data['weight'],
                pokemon_data.get('base_experience'),
                pokemon_data['order'],
                pokemon_data['is_default']
            ))
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert Pokemon {pokemon_data.get('name', 'Unknown')}: {e}")
            return False
    
    def insert_pokemon_types(self, cursor, pokemon_id: int, types_data: List[Dict]) -> bool:
        """Insert Pokemon type information"""
        try:
            # Clear existing types for this Pokemon
            cursor.execute("DELETE FROM raw.pokemon_types WHERE pokemon_id = %s", (pokemon_id,))
            
            insert_sql = """
                INSERT INTO raw.pokemon_types (pokemon_id, type_id, type_name, slot)
                VALUES (%s, %s, %s, %s)
            """
            
            for type_info in types_data:
                type_url = type_info['type']['url']
                type_id = int(type_url.split('/')[-2])  # Extract ID from URL
                
                cursor.execute(insert_sql, (
                    pokemon_id,
                    type_id,
                    type_info['type']['name'],
                    type_info['slot']
                ))
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert types for Pokemon {pokemon_id}: {e}")
            return False
    
    def insert_pokemon_stats(self, cursor, pokemon_id: int, stats_data: List[Dict]) -> bool:
        """Insert Pokemon base stats"""
        try:
            # Clear existing stats for this Pokemon
            cursor.execute("DELETE FROM raw.pokemon_stats WHERE pokemon_id = %s", (pokemon_id,))
            
            insert_sql = """
                INSERT INTO raw.pokemon_stats (pokemon_id, stat_name, base_stat, effort)
                VALUES (%s, %s, %s, %s)
            """
            
            for stat_info in stats_data:
                cursor.execute(insert_sql, (
                    pokemon_id,
                    stat_info['stat']['name'],
                    stat_info['base_stat'],
                    stat_info['effort']
                ))
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert stats for Pokemon {pokemon_id}: {e}")
            return False
    
    def insert_pokemon_abilities(self, cursor, pokemon_id: int, abilities_data: List[Dict]) -> bool:
        """Insert Pokemon abilities"""
        try:
            # Clear existing abilities for this Pokemon
            cursor.execute("DELETE FROM raw.pokemon_abilities WHERE pokemon_id = %s", (pokemon_id,))
            
            insert_sql = """
                INSERT INTO raw.pokemon_abilities (pokemon_id, ability_id, ability_name, is_hidden, slot)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            for ability_info in abilities_data:
                ability_url = ability_info['ability']['url']
                ability_id = int(ability_url.split('/')[-2])  # Extract ID from URL
                
                cursor.execute(insert_sql, (
                    pokemon_id,
                    ability_id,
                    ability_info['ability']['name'],
                    ability_info['is_hidden'],
                    ability_info['slot']
                ))
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert abilities for Pokemon {pokemon_id}: {e}")
            return False
    
    def process_pokemon(self, pokemon_id: int) -> bool:
        """Process a single Pokemon - fetch data and store in database"""
        pokemon_data = self.get_pokemon_data(pokemon_id)
        if not pokemon_data:
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Insert base Pokemon data
            success = self.insert_pokemon_base(cursor, pokemon_data)
            if not success:
                cursor.close()
                return False
            
            # Insert Pokemon types
            success = self.insert_pokemon_types(cursor, pokemon_id, pokemon_data['types'])
            if not success:
                cursor.close()
                return False
            
            # Insert Pokemon stats
            success = self.insert_pokemon_stats(cursor, pokemon_id, pokemon_data['stats'])
            if not success:
                cursor.close()
                return False
            
            # Insert Pokemon abilities
            success = self.insert_pokemon_abilities(cursor, pokemon_id, pokemon_data['abilities'])
            if not success:
                cursor.close()
                return False
            
            # Commit transaction
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Successfully processed Pokemon {pokemon_id}: {pokemon_data['name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process Pokemon {pokemon_id}: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def collect_pokemon_batch(self, start_id: int = 1, limit: int = 151, delay: float = 0.1) -> Tuple[int, int]:
        """
        Collect a batch of Pokemon data
        
        Args:
            start_id: Starting Pokemon ID
            limit: Maximum number of Pokemon to collect
            delay: Delay between API calls (rate limiting)
            
        Returns:
            Tuple of (successful_count, total_attempted)
        """
        successful = 0
        total = 0
        
        logger.info(f"Starting collection of {limit} Pokemon from ID {start_id}")
        
        for pokemon_id in range(start_id, start_id + limit):
            total += 1
            
            if self.process_pokemon(pokemon_id):
                successful += 1
            
            # Rate limiting
            time.sleep(delay)
            
            # Progress logging
            if total % 10 == 0:
                logger.info(f"Progress: {successful}/{total} Pokemon collected successfully")
        
        logger.info(f"Collection complete: {successful}/{total} Pokemon collected successfully")
        return successful, total
    
    def get_collection_stats(self) -> Dict[str, int]:
        """Get current collection statistics"""
        try:
            cursor = self.connection.cursor()
            
            stats = {}
            
            # Count Pokemon
            cursor.execute("SELECT COUNT(*) FROM raw.pokemon")
            stats['total_pokemon'] = cursor.fetchone()[0]
            
            # Count types
            cursor.execute("SELECT COUNT(*) FROM raw.pokemon_types")
            stats['total_type_entries'] = cursor.fetchone()[0]
            
            # Count stats
            cursor.execute("SELECT COUNT(*) FROM raw.pokemon_stats")
            stats['total_stat_entries'] = cursor.fetchone()[0]
            
            # Count abilities
            cursor.execute("SELECT COUNT(*) FROM raw.pokemon_abilities")
            stats['total_ability_entries'] = cursor.fetchone()[0]
            
            cursor.close()
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get collection stats: {e}")
            return {}

def main():
    parser = argparse.ArgumentParser(description='Pokemon API Data Collector')
    parser.add_argument('--limit', type=int, default=151, 
                       help='Number of Pokemon to collect (default: 151)')
    parser.add_argument('--start', type=int, default=1,
                       help='Starting Pokemon ID (default: 1)')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='Delay between API calls in seconds (default: 0.1)')
    parser.add_argument('--all', action='store_true',
                       help='Collect all Pokemon (overrides --limit)')
    
    args = parser.parse_args()
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'pokemon_analytics',
        'user': 'pokemon_user',
        'password': 'pokemon_pass'
    }
    
    # Initialize collector
    collector = PokeAPICollector(db_config)
    
    try:
        # Connect to database
        collector.connect_db()
        
        # Get initial stats
        initial_stats = collector.get_collection_stats()
        logger.info(f"Initial database stats: {initial_stats}")
        
        # Determine collection parameters
        if args.all:
            # Note: As of 2024, there are ~1010 Pokemon in PokeAPI
            limit = 1010
            logger.info("Collecting ALL Pokemon - this may take a while!")
        else:
            limit = args.limit
        
        # Collect Pokemon data
        start_time = datetime.now()
        successful, total = collector.collect_pokemon_batch(
            start_id=args.start,
            limit=limit,
            delay=args.delay
        )
        end_time = datetime.now()
        
        # Get final stats
        final_stats = collector.get_collection_stats()
        
        # Report results
        duration = (end_time - start_time).total_seconds()
        logger.info(f"\n=== COLLECTION SUMMARY ===")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Pokemon collected: {successful}/{total}")
        logger.info(f"Success rate: {(successful/total)*100:.1f}%")
        logger.info(f"Final database stats: {final_stats}")
        
        # Test Phase 2 completion
        if successful >= 151:
            logger.info("🎉 Phase 2 COMPLETED: Successfully collected 151+ Pokemon!")
        else:
            logger.warning("⚠️  Phase 2 INCOMPLETE: Less than 151 Pokemon collected")
        
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        return 1
    
    finally:
        collector.close_db()
    
    return 0

if __name__ == "__main__":
    exit(main())