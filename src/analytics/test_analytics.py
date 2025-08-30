#!/usr/bin/env python3
"""Quick test of analytics data access"""

import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = "postgresql://pokemon_user:pokemon_pass@localhost:5432/pokemon_analytics"
engine = create_engine(DATABASE_URL)

try:
    # Test data loading
    logger.info("Testing data loading...")
    
    pokemon_query = "SELECT * FROM analytics.dim_pokemon_enhanced LIMIT 5"
    pokemon_df = pd.read_sql(pokemon_query, engine)
    
    usage_query = "SELECT * FROM analytics.fact_usage_enhanced LIMIT 5"
    usage_df = pd.read_sql(usage_query, engine)
    
    logger.info(f"Pokemon columns: {list(pokemon_df.columns)}")
    logger.info(f"Usage columns: {list(usage_df.columns)}")
    
    # Test merge
    logger.info("Testing merge...")
    merged = usage_df.merge(
        pokemon_df[['name_clean', 'total_stats', 'hp', 'attack', 'primary_type']], 
        left_on='pokemon_name', 
        right_on='name_clean', 
        how='inner'
    )
    
    logger.info(f"Merged shape: {merged.shape}")
    logger.info(f"Merged columns: {list(merged.columns)}")
    
    if not merged.empty:
        logger.info(f"Sample merged data:")
        logger.info(merged.head())
        
        # Test the problematic line
        if 'total_stats' in merged.columns:
            logger.info(f"total_stats exists! Sample values: {merged['total_stats'].head()}")
        else:
            logger.error("total_stats column not found after merge!")
    
except Exception as e:
    logger.error(f"Test failed: {e}")
    import traceback
    traceback.print_exc()