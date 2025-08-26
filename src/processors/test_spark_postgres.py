#!/usr/bin/env python3
"""
PySpark PostgreSQL Connectivity Test

Tests basic connectivity between PySpark and PostgreSQL database.
This is Phase 3 of the Pokemon Analytics project.

Usage:
    spark-submit --packages org.postgresql:postgresql:42.7.0 test_spark_postgres.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkPostgresTest:
    """Test PySpark connectivity with PostgreSQL"""
    
    def __init__(self):
        self.spark = None
        self.postgres_config = {
            "url": "jdbc:postgresql://postgres:5432/pokemon_analytics",
            "user": "pokemon_user",
            "password": "pokemon_pass",
            "driver": "org.postgresql.Driver"
        }
        
    def create_spark_session(self):
        """Create Spark session with PostgreSQL connector"""
        try:
            self.spark = SparkSession.builder \
                .appName("Pokemon Analytics - Postgres Test") \
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("✅ Spark session created successfully")
            logger.info(f"Spark version: {self.spark.version}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            return False
    
    def test_postgres_connection(self):
        """Test reading data from PostgreSQL"""
        try:
            # Read Pokemon data
            pokemon_df = self.spark.read \
                .format("jdbc") \
                .options(**self.postgres_config) \
                .option("dbtable", "raw.pokemon") \
                .load()
            
            # Basic statistics
            total_pokemon = pokemon_df.count()
            logger.info(f"✅ Successfully read {total_pokemon} Pokemon from PostgreSQL")
            
            if total_pokemon > 0:
                # Show sample data
                logger.info("Sample Pokemon data:")
                pokemon_df.select("id", "name", "height", "weight").show(5, truncate=False)
                
                # Basic stats
                stats = pokemon_df.agg(
                    avg("height").alias("avg_height"),
                    avg("weight").alias("avg_weight"),
                    spark_min("id").alias("min_id"),
                    spark_max("id").alias("max_id")
                ).collect()[0]
                
                logger.info(f"Pokemon Statistics:")
                logger.info(f"  Average height: {stats['avg_height']:.2f}")
                logger.info(f"  Average weight: {stats['avg_weight']:.2f}")
                logger.info(f"  ID range: {stats['min_id']} - {stats['max_id']}")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to read from PostgreSQL: {e}")
            return False
    
    def test_pokemon_types_analysis(self):
        """Test joining tables and basic analysis"""
        try:
            # Read Pokemon and types tables
            pokemon_df = self.spark.read \
                .format("jdbc") \
                .options(**self.postgres_config) \
                .option("dbtable", "raw.pokemon") \
                .load()
            
            types_df = self.spark.read \
                .format("jdbc") \
                .options(**self.postgres_config) \
                .option("dbtable", "raw.pokemon_types") \
                .load()
            
            # Join and analyze types
            type_counts = types_df \
                .groupBy("type_name") \
                .agg(count("pokemon_id").alias("pokemon_count")) \
                .orderBy(col("pokemon_count").desc())
            
            logger.info("✅ Pokemon type distribution:")
            type_counts.show(10, truncate=False)
            
            # Test writing back to PostgreSQL (to staging schema)
            type_counts.write \
                .format("jdbc") \
                .options(**self.postgres_config) \
                .option("dbtable", "staging.type_analysis") \
                .mode("overwrite") \
                .save()
            
            logger.info("✅ Successfully wrote analysis results to staging.type_analysis")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed type analysis: {e}")
            return False
    
    def test_pokemon_stats_processing(self):
        """Test processing Pokemon stats data"""
        try:
            # Read stats data
            stats_df = self.spark.read \
                .format("jdbc") \
                .options(**self.postgres_config) \
                .option("dbtable", "raw.pokemon_stats") \
                .load()
            
            # Pivot stats to columns (like traditional Pokemon stats table)
            pokemon_stats = stats_df \
                .groupBy("pokemon_id") \
                .pivot("stat_name") \
                .agg(avg("base_stat").alias("base_stat"))
            
            logger.info("✅ Pokemon stats processing sample:")
            pokemon_stats.show(5, truncate=False)
            
            # Calculate total stats
            stat_columns = ["hp", "attack", "defense", "special-attack", "special-defense", "speed"]
            
            # Add total stats column
            total_stats_expr = sum(col(stat_col) for stat_col in stat_columns if stat_col in pokemon_stats.columns)
            
            pokemon_with_totals = pokemon_stats.withColumn("total_stats", total_stats_expr)
            
            # Find strongest Pokemon
            strongest = pokemon_with_totals.orderBy(col("total_stats").desc())
            
            logger.info("✅ Top 5 Pokemon by total stats:")
            strongest.select("pokemon_id", "total_stats", "hp", "attack", "defense").show(5)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed stats processing: {e}")
            return False
    
    def cleanup(self):
        """Clean up Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("✅ Spark session stopped")

def main():
    """Run all PySpark tests"""
    logger.info("🚀 Starting PySpark-PostgreSQL connectivity tests...")
    
    tester = SparkPostgresTest()
    success_count = 0
    total_tests = 4
    
    try:
        # Test 1: Create Spark session
        if tester.create_spark_session():
            success_count += 1
        
        # Test 2: Basic PostgreSQL connection
        if tester.test_postgres_connection():
            success_count += 1
        
        # Test 3: Types analysis (join + aggregation)
        if tester.test_pokemon_types_analysis():
            success_count += 1
        
        # Test 4: Stats processing (pivot + calculations)
        if tester.test_pokemon_stats_processing():
            success_count += 1
        
        # Results
        logger.info("\n" + "="*50)
        logger.info("🎯 PHASE 3 TEST RESULTS")
        logger.info("="*50)
        logger.info(f"Tests passed: {success_count}/{total_tests}")
        logger.info(f"Success rate: {(success_count/total_tests)*100:.1f}%")
        
        if success_count == total_tests:
            logger.info("🎉 Phase 3 COMPLETED: PySpark-PostgreSQL integration working!")
        else:
            logger.warning(f"⚠️  Phase 3 INCOMPLETE: {total_tests - success_count} tests failed")
        
        return success_count == total_tests
        
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}")
        return False
        
    finally:
        tester.cleanup()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)