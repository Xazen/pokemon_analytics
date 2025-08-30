#!/usr/bin/env python3
"""Debug the merge issue"""

import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://pokemon_user:pokemon_pass@localhost:5432/pokemon_analytics"
engine = create_engine(DATABASE_URL)

try:
    # Load data
    pokemon_df = pd.read_sql("SELECT * FROM analytics.dim_pokemon_enhanced LIMIT 5", engine)
    usage_df = pd.read_sql("SELECT * FROM analytics.fact_usage_enhanced LIMIT 5", engine)
    
    logger.info(f"Pokemon cols: {list(pokemon_df.columns)}")
    logger.info(f"Usage cols: {list(usage_df.columns)}")
    
    # Try merge
    available_columns = list(pokemon_df.columns)
    merge_columns = ['name_clean']
    
    for col in ['total_stats', 'hp', 'attack', 'defense', 'special_attack', 'special_defense', 'speed', 'primary_type', 'generation']:
        if col in available_columns:
            merge_columns.append(col)
    
    logger.info(f"Merge columns: {merge_columns}")
    
    ml_data = usage_df.merge(
        pokemon_df[merge_columns],
        left_on='pokemon_name',
        right_on='name_clean',
        how='inner'
    )
    
    logger.info(f"After merge columns: {list(ml_data.columns)}")
    logger.info(f"Merge shape: {ml_data.shape}")
    
    # Check for the problematic column access
    features = ['total_stats', 'hp', 'attack', 'defense', 'special_attack', 'special_defense', 'speed', 'generation']
    
    # Test each feature
    for feat in features:
        if feat in ml_data.columns:
            logger.info(f"✅ {feat} exists")
        else:
            logger.error(f"❌ {feat} missing")
            # Check for variations
            similar = [col for col in ml_data.columns if feat in col]
            if similar:
                logger.info(f"   Similar columns: {similar}")
    
except Exception as e:
    logger.error(f"Debug failed: {e}")
    import traceback
    traceback.print_exc()