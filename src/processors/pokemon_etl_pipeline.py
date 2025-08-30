#!/usr/bin/env python3
"""
Pokemon Analytics ETL Pipeline

Comprehensive ETL pipeline for Pokemon competitive analytics.
This is Phase 5 of the Pokemon Analytics project - Data Cleaning & ETL.

Features:
- Data cleaning and standardization
- Dimensional modeling for analytics
- Competitive viability scoring
- Type effectiveness calculations
- Meta trend analysis

Usage:
    spark-submit --packages org.postgresql:postgresql:42.7.0 pokemon_etl_pipeline.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, upper, lower, round as spark_round,
    avg, max as spark_max, min as spark_min, count, sum as spark_sum,
    dense_rank, row_number, lag, lead, coalesce, isnan, isnull,
    current_timestamp, lit, desc, asc, year, month, dayofmonth,
    split, explode, collect_list, array_contains, size, concat, countDistinct
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PokemonETL:
    """Pokemon Analytics ETL Pipeline"""
    
    def __init__(self):
        self.spark = None
        self.postgres_config = {
            "url": "jdbc:postgresql://postgres:5432/pokemon_analytics",
            "user": "pokemon_user",
            "password": "pokemon_pass",
            "driver": "org.postgresql.Driver"
        }
    
    def create_spark_session(self):
        """Initialize Spark session with optimizations"""
        self.spark = SparkSession.builder \
            .appName("Pokemon Analytics ETL Pipeline") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session created with optimizations")
    
    def extract_raw_data(self):
        """Extract data from raw tables"""
        logger.info("📊 Extracting raw data from PostgreSQL...")
        
        # Pokemon base data
        pokemon_df = self.spark.read \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "raw.pokemon") \
            .load()
        
        # Pokemon types
        types_df = self.spark.read \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "raw.pokemon_types") \
            .load()
        
        # Pokemon stats
        stats_df = self.spark.read \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "raw.pokemon_stats") \
            .load()
        
        # Pokemon abilities
        abilities_df = self.spark.read \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "raw.pokemon_abilities") \
            .load()
        
        # Competitive usage data
        usage_df = self.spark.read \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "raw.usage_stats") \
            .load()
        
        logger.info(f"✅ Extracted data: {pokemon_df.count()} Pokemon, {usage_df.count()} usage records")
        
        return {
            "pokemon": pokemon_df,
            "types": types_df,
            "stats": stats_df,
            "abilities": abilities_df,
            "usage": usage_df
        }
    
    def clean_pokemon_data(self, raw_data):
        """Clean and standardize Pokemon base data"""
        logger.info("🧹 Cleaning Pokemon base data...")
        
        pokemon_df = raw_data["pokemon"]
        types_df = raw_data["types"]
        stats_df = raw_data["stats"]
        
        # Clean Pokemon names
        pokemon_clean = pokemon_df.select(
            col("id").alias("pokemon_id"),
            trim(regexp_replace(lower(col("name")), r"[^a-z0-9\-]", "")).alias("name_clean"),
            col("name").alias("name_original"),
            (col("height") / 10.0).alias("height_m"),  # Convert decimeters to meters
            (col("weight") / 10.0).alias("weight_kg"),  # Convert hectograms to kg
            coalesce(col("base_experience"), lit(0)).alias("base_experience"),
            col("order_id"),
            col("is_default"),
            col("created_at")
        ).filter(col("name").isNotNull())
        
        # Pivot stats to columns
        stats_pivot = stats_df.groupBy("pokemon_id").pivot("stat_name").agg(
            avg("base_stat").alias("base_stat")
        ).select(
            col("pokemon_id"),
            coalesce(col("hp"), lit(0)).alias("hp"),
            coalesce(col("attack"), lit(0)).alias("attack"),
            coalesce(col("defense"), lit(0)).alias("defense"),
            coalesce(col("special-attack"), lit(0)).alias("special_attack"),
            coalesce(col("special-defense"), lit(0)).alias("special_defense"),
            coalesce(col("speed"), lit(0)).alias("speed")
        )
        
        # Add calculated stats
        stats_enhanced = stats_pivot.withColumn(
            "total_stats",
            col("hp") + col("attack") + col("defense") + 
            col("special_attack") + col("special_defense") + col("speed")
        ).withColumn(
            "physical_power", (col("attack") + col("defense")) / 2
        ).withColumn(
            "special_power", (col("special_attack") + col("special_defense")) / 2
        ).withColumn(
            "bulk", (col("hp") + col("defense") + col("special_defense")) / 3
        ).withColumn(
            "offensive_power", (col("attack") + col("special_attack")) / 2
        )
        
        # Get primary and secondary types
        types_ranked = types_df.withColumn(
            "type_rank", 
            row_number().over(Window.partitionBy("pokemon_id").orderBy("slot"))
        )
        
        primary_types = types_ranked.filter(col("type_rank") == 1) \
            .select(col("pokemon_id"), col("type_name").alias("primary_type"))
        
        secondary_types = types_ranked.filter(col("type_rank") == 2) \
            .select(col("pokemon_id"), col("type_name").alias("secondary_type"))
        
        # Join everything together
        pokemon_complete = pokemon_clean \
            .join(stats_enhanced, "pokemon_id", "left") \
            .join(primary_types, "pokemon_id", "left") \
            .join(secondary_types, "pokemon_id", "left") \
            .withColumn(
                "type_combination",
                when(col("secondary_type").isNotNull(),
                     concat(col("primary_type"), lit("/"), col("secondary_type")))
                .otherwise(col("primary_type"))
            )
        
        logger.info(f"✅ Cleaned Pokemon data: {pokemon_complete.count()} records")
        return pokemon_complete
    
    def clean_usage_data(self, raw_data):
        """Clean competitive usage data"""
        logger.info("🧹 Cleaning competitive usage data...")
        
        usage_df = raw_data["usage"]
        
        # Clean and standardize usage data
        usage_clean = usage_df.select(
            col("id").alias("usage_id"),
            trim(regexp_replace(lower(col("pokemon_name")), r"[^a-z0-9\-]", "")).alias("pokemon_name_clean"),
            col("pokemon_name").alias("pokemon_name_original"),
            upper(col("format")).alias("format"),
            col("usage_percentage").cast(DecimalType(8,4)).alias("usage_percentage"),
            col("rank_position").cast(IntegerType()).alias("rank_position"),
            col("month_year"),
            coalesce(col("raw_count"), lit(0)).cast(LongType()).alias("raw_count"),
            col("created_at")
        ).filter(
            col("pokemon_name").isNotNull() &
            col("format").isNotNull() &
            col("usage_percentage").isNotNull() &
            (col("usage_percentage") >= 0) &
            (col("usage_percentage") <= 100)
        )
        
        # Add usage tiers
        usage_tiered = usage_clean.withColumn(
            "usage_tier",
            when(col("usage_percentage") >= 20, "S-Tier")
            .when(col("usage_percentage") >= 10, "A-Tier")
            .when(col("usage_percentage") >= 5, "B-Tier")
            .when(col("usage_percentage") >= 1, "C-Tier")
            .otherwise("D-Tier")
        ).withColumn(
            "viability_score",
            spark_round(
                (100 - col("rank_position")) * 0.3 + 
                col("usage_percentage") * 0.7, 2
            )
        )
        
        logger.info(f"✅ Cleaned usage data: {usage_clean.count()} records")
        return usage_tiered
    
    def create_pokemon_dimension(self, pokemon_clean):
        """Create Pokemon dimension table"""
        logger.info("📊 Creating Pokemon dimension...")
        
        # Add generation info (simplified)
        dim_pokemon = pokemon_clean.withColumn(
            "generation",
            when(col("pokemon_id") <= 151, 1)
            .when(col("pokemon_id") <= 251, 2)
            .when(col("pokemon_id") <= 386, 3)
            .when(col("pokemon_id") <= 493, 4)
            .when(col("pokemon_id") <= 649, 5)
            .when(col("pokemon_id") <= 721, 6)
            .when(col("pokemon_id") <= 809, 7)
            .when(col("pokemon_id") <= 905, 8)
            .otherwise(9)
        ).withColumn(
            "is_legendary",
            # Simplified legendary detection based on base stats
            when(col("total_stats") >= 600, True).otherwise(False)
        ).withColumn(
            "stat_tier",
            when(col("total_stats") >= 600, "Legendary")
            .when(col("total_stats") >= 520, "High")
            .when(col("total_stats") >= 450, "Medium")
            .when(col("total_stats") >= 350, "Low")
            .otherwise("Very Low")
        )
        
        logger.info(f"✅ Created Pokemon dimension: {dim_pokemon.count()} Pokemon")
        return dim_pokemon
    
    def create_usage_facts(self, usage_clean, dim_pokemon):
        """Create usage fact table with enriched data"""
        logger.info("📊 Creating usage fact table...")
        
        # Join usage data with Pokemon dimension
        fact_usage = usage_clean.alias("u") \
            .join(
                dim_pokemon.select("pokemon_id", "name_clean", "generation", "stat_tier", "total_stats").alias("p"),
                col("u.pokemon_name_clean") == col("p.name_clean"),
                "left"
            ).select(
                col("u.usage_id"),
                col("p.pokemon_id"),
                col("u.pokemon_name_clean").alias("pokemon_name"),
                col("u.format"),
                col("u.month_year"),
                col("u.usage_percentage"),
                col("u.rank_position"),
                col("u.raw_count"),
                col("u.usage_tier"),
                col("u.viability_score"),
                coalesce(col("p.generation"), lit(0)).alias("generation"),
                coalesce(col("p.stat_tier"), lit("Unknown")).alias("stat_tier"),
                coalesce(col("p.total_stats"), lit(0)).alias("total_stats"),
                col("u.created_at")
            )
        
        # Add competitive metrics
        fact_enriched = fact_usage.withColumn(
            "meta_dominance",
            when(col("usage_percentage") >= 30, "Dominant")
            .when(col("usage_percentage") >= 20, "Popular")
            .when(col("usage_percentage") >= 10, "Viable")
            .when(col("usage_percentage") >= 5, "Niche")
            .otherwise("Rare")
        ).withColumn(
            "rank_percentile",
            spark_round((100 - col("rank_position")) / 100.0 * 100, 1)
        )
        
        logger.info(f"✅ Created usage facts: {fact_enriched.count()} records")
        return fact_enriched
    
    def analyze_type_effectiveness(self, dim_pokemon, fact_usage):
        """Analyze type effectiveness in competitive play"""
        logger.info("🔍 Analyzing type effectiveness...")
        
        # Type usage analysis  
        type_effectiveness = dim_pokemon.alias("d") \
            .join(fact_usage.alias("f"), "pokemon_id", "inner") \
            .groupBy("primary_type", "format") \
            .agg(
                count("pokemon_id").alias("pokemon_count"),
                avg("usage_percentage").alias("avg_usage"),
                avg("viability_score").alias("avg_viability"),
                spark_max("usage_percentage").alias("max_usage"),
                avg(col("d.total_stats")).alias("avg_base_stats")
            ).withColumn(
                "type_meta_score",
                spark_round(
                    col("avg_usage") * 0.4 + 
                    col("avg_viability") * 0.3 + 
                    (col("avg_base_stats") / 600 * 100) * 0.3, 2
                )
            ).orderBy(desc("type_meta_score"))
        
        logger.info(f"✅ Type effectiveness analysis: {type_effectiveness.count()} type-format combinations")
        return type_effectiveness
    
    def create_meta_trends(self, fact_usage):
        """Create meta trend analysis"""
        logger.info("📈 Creating meta trend analysis...")
        
        # Monthly usage trends
        meta_trends = fact_usage \
            .filter(col("rank_position") <= 20) \
            .groupBy("pokemon_name", "format", "month_year") \
            .agg(
                avg("usage_percentage").alias("usage_percentage"),
                avg("rank_position").alias("rank_position"),
                avg("viability_score").alias("viability_score")
            )
        
        # Add trend indicators using window functions
        window_spec = Window.partitionBy("pokemon_name", "format").orderBy("month_year")
        
        trends_with_movement = meta_trends.withColumn(
            "previous_usage", lag("usage_percentage").over(window_spec)
        ).withColumn(
            "previous_rank", lag("rank_position").over(window_spec)
        ).withColumn(
            "usage_trend",
            when(col("previous_usage").isNotNull(),
                 col("usage_percentage") - col("previous_usage"))
            .otherwise(lit(0.0))
        ).withColumn(
            "rank_trend",
            when(col("previous_rank").isNotNull(),
                 col("previous_rank") - col("rank_position"))  # Positive = rank improvement
            .otherwise(lit(0))
        ).withColumn(
            "trend_direction",
            when(col("usage_trend") > 0.5, "Rising")
            .when(col("usage_trend") < -0.5, "Falling")
            .otherwise("Stable")
        )
        
        logger.info(f"✅ Meta trends analysis: {trends_with_movement.count()} trend records")
        return trends_with_movement
    
    def write_to_analytics_schema(self, dim_pokemon, fact_usage, type_effectiveness, meta_trends):
        """Write processed data to analytics schema"""
        logger.info("💾 Writing data to analytics schema...")
        
        # Write Pokemon dimension
        dim_pokemon.write \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "analytics.dim_pokemon_enhanced") \
            .mode("overwrite") \
            .save()
        
        # Write usage facts
        fact_usage.write \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "analytics.fact_usage_enhanced") \
            .mode("overwrite") \
            .save()
        
        # Write type effectiveness
        type_effectiveness.write \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "analytics.type_effectiveness") \
            .mode("overwrite") \
            .save()
        
        # Write meta trends
        meta_trends.write \
            .format("jdbc") \
            .options(**self.postgres_config) \
            .option("dbtable", "analytics.meta_trends") \
            .mode("overwrite") \
            .save()
        
        logger.info("✅ All analytics tables written successfully")
    
    def generate_summary_report(self, dim_pokemon, fact_usage):
        """Generate ETL summary report"""
        logger.info("📋 Generating ETL summary report...")
        
        # Pokemon summary
        pokemon_summary = dim_pokemon.agg(
            count("pokemon_id").alias("total_pokemon"),
            countDistinct("primary_type").alias("unique_types"),
            countDistinct("generation").alias("generations"),
            avg("total_stats").alias("avg_total_stats")
        ).collect()[0]
        
        # Usage summary
        usage_summary = fact_usage.agg(
            count("usage_id").alias("total_usage_records"),
            countDistinct("format").alias("formats_analyzed"),
            countDistinct("pokemon_name").alias("competitive_pokemon"),
            avg("usage_percentage").alias("avg_usage_percentage")
        ).collect()[0]
        
        # Top competitive Pokemon
        top_pokemon = fact_usage.orderBy(desc("viability_score")).limit(5).collect()
        
        summary = {
            "etl_timestamp": datetime.now(),
            "pokemon_data": {
                "total_pokemon": pokemon_summary["total_pokemon"],
                "unique_types": pokemon_summary["unique_types"],
                "generations": pokemon_summary["generations"],
                "avg_total_stats": round(float(pokemon_summary["avg_total_stats"]), 2)
            },
            "competitive_data": {
                "usage_records": usage_summary["total_usage_records"],
                "formats": usage_summary["formats_analyzed"],
                "competitive_pokemon": usage_summary["competitive_pokemon"],
                "avg_usage": round(float(usage_summary["avg_usage_percentage"]), 2)
            },
            "top_pokemon": [
                {
                    "name": row["pokemon_name"],
                    "format": row["format"],
                    "usage": float(row["usage_percentage"]),
                    "viability_score": float(row["viability_score"])
                } for row in top_pokemon
            ]
        }
        
        return summary
    
    def run_etl_pipeline(self):
        """Execute complete ETL pipeline"""
        logger.info("🚀 Starting Pokemon Analytics ETL Pipeline...")
        
        try:
            # Initialize Spark
            self.create_spark_session()
            
            # Extract raw data
            raw_data = self.extract_raw_data()
            
            # Clean and transform data
            pokemon_clean = self.clean_pokemon_data(raw_data)
            usage_clean = self.clean_usage_data(raw_data)
            
            # Create dimensional model
            dim_pokemon = self.create_pokemon_dimension(pokemon_clean)
            fact_usage = self.create_usage_facts(usage_clean, dim_pokemon)
            
            # Generate analytics
            type_effectiveness = self.analyze_type_effectiveness(dim_pokemon, fact_usage)
            meta_trends = self.create_meta_trends(fact_usage)
            
            # Write to analytics schema
            self.write_to_analytics_schema(dim_pokemon, fact_usage, type_effectiveness, meta_trends)
            
            # Generate summary
            summary = self.generate_summary_report(dim_pokemon, fact_usage)
            
            logger.info("\n" + "="*60)
            logger.info("🎯 ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            logger.info(f"Pokemon processed: {summary['pokemon_data']['total_pokemon']}")
            logger.info(f"Competitive records: {summary['competitive_data']['usage_records']}")
            logger.info(f"Formats analyzed: {summary['competitive_data']['formats']}")
            logger.info(f"Top Pokemon: {summary['top_pokemon'][0]['name']} ({summary['top_pokemon'][0]['usage']:.1f}%)")
            logger.info("🎉 Phase 5 COMPLETED: ETL Pipeline with data cleaning!")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ ETL Pipeline failed: {e}")
            return False
            
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("✅ Spark session closed")

def main():
    """Main ETL execution function"""
    etl = PokemonETL()
    success = etl.run_etl_pipeline()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())