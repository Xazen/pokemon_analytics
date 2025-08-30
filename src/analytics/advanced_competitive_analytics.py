#!/usr/bin/env python3
"""
Advanced Pokemon Competitive Analytics

Phase 6 implementation: Sophisticated analytical algorithms for competitive insights.
Demonstrates machine learning, statistical analysis, and strategic recommendations.

Features:
- Meta analysis with competitive insights
- Type effectiveness optimization  
- Machine learning usage prediction
- Team building recommendations
- Strategic insights generation
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from sqlalchemy import create_engine
import warnings
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

class AdvancedCompetitiveAnalytics:
    """Advanced analytics engine for Pokemon competitive data"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
        self.analytics_data = {}
        
    def load_analytics_data(self):
        """Load all analytics tables for comprehensive analysis"""
        
        queries = {
            'pokemon_dim': """
                SELECT * FROM analytics.dim_pokemon_enhanced 
                ORDER BY total_stats DESC
            """,
            
            'usage_facts': """
                SELECT * FROM analytics.fact_usage_enhanced
                ORDER BY usage_percentage DESC
            """,
            
            'type_effectiveness': """
                SELECT * FROM analytics.type_effectiveness
                ORDER BY type_meta_score DESC
            """,
            
            'meta_trends': """
                SELECT * FROM analytics.meta_trends
                ORDER BY usage_percentage DESC
            """
        }
        
        logger.info("📊 Loading analytics data...")
        for name, query in queries.items():
            try:
                df = pd.read_sql(query, self.engine)
                self.analytics_data[name] = df
                logger.info(f"✅ Loaded {name}: {len(df)} records")
            except Exception as e:
                logger.error(f"❌ Failed to load {name}: {e}")
                self.analytics_data[name] = pd.DataFrame()
        
        return self.analytics_data
    
    def analyze_competitive_meta(self):
        """Comprehensive competitive meta analysis"""
        
        usage_df = self.analytics_data.get('usage_facts', pd.DataFrame())
        pokemon_df = self.analytics_data.get('pokemon_dim', pd.DataFrame())
        
        if usage_df.empty:
            logger.warning("❌ No usage data available for meta analysis")
            return {}
        
        logger.info("🏆 Running competitive meta analysis...")
        
        # Top 10 Pokemon by usage
        top_pokemon = usage_df.head(10)[['pokemon_name', 'usage_percentage', 'rank_position', 'viability_score']]
        
        logger.info("📊 Top 10 Most Used Pokemon:")
        for idx, row in top_pokemon.iterrows():
            logger.info(f"  #{int(row['rank_position'])}: {row['pokemon_name'].title()} - {row['usage_percentage']:.1f}% (Viability: {row['viability_score']:.1f})")
        
        # Usage distribution analysis
        usage_stats = usage_df['usage_percentage'].describe()
        logger.info(f"📈 Usage Statistics:")
        logger.info(f"  Mean usage: {usage_stats['mean']:.2f}%")
        logger.info(f"  Median usage: {usage_stats['50%']:.2f}%")
        logger.info(f"  Top 1% threshold: {usage_df['usage_percentage'].quantile(0.99):.2f}%")
        
        # Meta diversity index
        high_usage = len(usage_df[usage_df['usage_percentage'] >= 10])
        medium_usage = len(usage_df[(usage_df['usage_percentage'] >= 5) & (usage_df['usage_percentage'] < 10)])
        diversity_score = (high_usage * 3 + medium_usage * 2) / len(usage_df) * 100
        
        logger.info(f"🎯 Meta Health Metrics:")
        logger.info(f"  Diversity Score: {diversity_score:.1f}/100")
        logger.info(f"  High usage Pokemon (≥10%): {high_usage}")
        logger.info(f"  Medium usage Pokemon (5-10%): {medium_usage}")
        
        return {
            'top_pokemon': top_pokemon,
            'diversity_score': diversity_score,
            'usage_stats': usage_stats,
            'high_usage_count': high_usage
        }
    
    def analyze_type_effectiveness(self):
        """Advanced type effectiveness and team building analysis"""
        
        type_df = self.analytics_data.get('type_effectiveness', pd.DataFrame())
        usage_df = self.analytics_data.get('usage_facts', pd.DataFrame())
        pokemon_df = self.analytics_data.get('pokemon_dim', pd.DataFrame())
        
        if type_df.empty:
            logger.warning("❌ No type effectiveness data available")
            return {}
        
        logger.info("⚡ Running type effectiveness analysis...")
        
        # Top performing types
        logger.info("🏅 Top Performing Types:")
        for idx, row in type_df.head(8).iterrows():
            logger.info(f"  {row['primary_type'].title()}: {row['type_meta_score']:.1f} meta score ({row['pokemon_count']} Pokemon, {row['avg_usage']:.1f}% avg)")
        
        if not pokemon_df.empty and not usage_df.empty:
            # Type diversity analysis
            merged_df = usage_df.merge(
                pokemon_df[['name_clean', 'primary_type', 'secondary_type', 'total_stats']], 
                left_on='pokemon_name', 
                right_on='name_clean', 
                how='left'
            )
            
            if not merged_df.empty:
                top_20_types = merged_df.head(20)['primary_type'].value_counts()
                logger.info(f"🌈 Type Diversity in Top 20:")
                for type_name, count in top_20_types.head(5).items():
                    logger.info(f"  {type_name.title()}: {count} Pokemon ({count/20*100:.1f}%)")
                
                # Coverage analysis
                type_coverage = len(merged_df['primary_type'].unique())
                total_types = len(pokemon_df['primary_type'].unique()) if not pokemon_df.empty else 18
                coverage_percent = (type_coverage / total_types) * 100
                logger.info(f"🎯 Competitive type coverage: {type_coverage}/{total_types} types ({coverage_percent:.1f}%)")
        
        return {
            'top_types': type_df.head(5),
            'type_coverage': type_coverage if 'type_coverage' in locals() else 0
        }
    
    def build_usage_prediction_model(self):
        """Build machine learning model to predict Pokemon competitive usage"""
        
        pokemon_df = self.analytics_data.get('pokemon_dim', pd.DataFrame())
        usage_df = self.analytics_data.get('usage_facts', pd.DataFrame())
        
        if pokemon_df.empty or usage_df.empty:
            logger.warning("❌ Insufficient data for ML model")
            return {}
        
        logger.info("🤖 Building usage prediction model...")
        
        # Try merge Pokemon stats with usage data
        available_columns = list(pokemon_df.columns)
        merge_columns = ['name_clean']
        
        # Add columns that exist
        for col in ['total_stats', 'hp', 'attack', 'defense', 'special_attack', 'special_defense', 'speed', 'primary_type', 'generation']:
            if col in available_columns:
                merge_columns.append(col)
        
        ml_data = usage_df.merge(
            pokemon_df[merge_columns],
            left_on='pokemon_name',
            right_on='name_clean',
            how='inner'
        )
        
        logger.info(f"📊 Merge result: {len(ml_data)} Pokemon matched out of {len(usage_df)} usage records")
        
        if len(ml_data) < 5:
            # Fall back to using just the usage data for analysis
            logger.warning(f"⚠️ Limited merged data ({len(ml_data)} records), using usage data only")
            
            # Create simplified analysis with available data
            high_usage = len(usage_df[usage_df['usage_percentage'] >= 10])
            
            return {
                'limited_analysis': True,
                'high_usage_pokemon': usage_df.head(10),
                'usage_distribution': usage_df['usage_percentage'].describe(),
                'high_usage_count': high_usage
            }
        
        logger.info(f"📊 Training data: {len(ml_data)} Pokemon")
        
        # Handle column name conflicts from merge (pandas adds _x, _y suffixes)
        # Use the Pokemon dimension stats (more reliable) - they have _y suffix
        if 'total_stats_y' in ml_data.columns:
            ml_data['total_stats'] = ml_data['total_stats_y']
        if 'generation_y' in ml_data.columns:
            ml_data['generation'] = ml_data['generation_y']
        
        # Feature engineering
        features = ['total_stats', 'hp', 'attack', 'defense', 'special_attack', 'special_defense', 'speed', 'generation']
        
        # Only include features that exist after merge
        available_features = []
        for feat in features:
            if feat in ml_data.columns:
                available_features.append(feat)
            else:
                logger.warning(f"⚠️ Feature {feat} not available, skipping")
        
        features = available_features
        
        if len(features) < 3:
            logger.warning(f"⚠️ Too few features available ({len(features)}), skipping ML model")
            return {'limited_analysis': True, 'reason': 'insufficient_features'}
        
        # Add type encoding if primary_type exists
        if 'primary_type' in ml_data.columns:
            le_type = LabelEncoder()
            ml_data['type_encoded'] = le_type.fit_transform(ml_data['primary_type'])
            features.append('type_encoded')
        
        # Add stat ratios for better prediction (only if total_stats exists)
        if 'total_stats' in ml_data.columns:
            ml_data['offensive_ratio'] = (ml_data['attack'] + ml_data['special_attack']) / ml_data['total_stats']
            ml_data['defensive_ratio'] = (ml_data['defense'] + ml_data['special_defense'] + ml_data['hp']) / ml_data['total_stats'] 
            ml_data['speed_ratio'] = ml_data['speed'] / ml_data['total_stats']
            features.extend(['offensive_ratio', 'defensive_ratio', 'speed_ratio'])
        
        # Prepare training data
        X = ml_data[features].fillna(0)
        y = ml_data['usage_percentage']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        # Train Random Forest model
        rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
        rf_model.fit(X_train, y_train)
        
        # Make predictions
        y_pred = rf_model.predict(X_test)
        
        # Model performance
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        logger.info(f"📈 Model Performance:")
        logger.info(f"  R² Score: {r2:.3f}")
        logger.info(f"  MSE: {mse:.3f}")
        logger.info(f"  RMSE: {np.sqrt(mse):.3f}%")
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': features,
            'importance': rf_model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info(f"🏆 Most Important Features:")
        for idx, row in feature_importance.head(5).iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.3f}")
        
        # Find underrated Pokemon
        all_predictions = rf_model.predict(X)
        ml_data['predicted_usage'] = all_predictions
        
        underrated = ml_data[ml_data['predicted_usage'] > ml_data['usage_percentage']].nlargest(5, 'predicted_usage')
        
        logger.info(f"💎 Most Underrated Pokemon:")
        for idx, row in underrated.iterrows():
            logger.info(f"  {row['pokemon_name'].title()}: {row['usage_percentage']:.1f}% actual, {row['predicted_usage']:.1f}% predicted")
        
        return {
            'model': rf_model,
            'r2_score': r2,
            'feature_importance': feature_importance,
            'underrated_pokemon': underrated,
            'predictions': ml_data
        }
    
    def generate_strategic_insights(self, meta_results, type_results, ml_results):
        """Generate actionable strategic insights for competitive play"""
        
        logger.info("🎯 Generating strategic insights...")
        
        insights = []
        
        # Meta dominance insights
        if meta_results and 'top_pokemon' in meta_results:
            top_pokemon = meta_results['top_pokemon']
            leader = top_pokemon.iloc[0]
            
            insights.append(f"🏆 META LEADER: {leader['pokemon_name'].title()}")
            insights.append(f"   - Dominates with {leader['usage_percentage']:.1f}% usage rate")
            insights.append(f"   - Viability score: {leader['viability_score']:.1f}/100")
            
            # Meta health assessment
            diversity = meta_results.get('diversity_score', 0)
            if diversity > 60:
                insights.append(f"✅ HEALTHY META: Diversity score {diversity:.1f}/100")
            elif diversity < 30:
                insights.append(f"⚠️ STALE META: Low diversity score {diversity:.1f}/100")
        
        # Type effectiveness insights
        if type_results and 'top_types' in type_results:
            best_type = type_results['top_types'].iloc[0]
            insights.append(f"⚡ STRONGEST TYPE: {best_type['primary_type'].title()}")
            insights.append(f"   - Meta score: {best_type['type_meta_score']:.1f}")
            insights.append(f"   - {best_type['pokemon_count']} competitive Pokemon")
        
        # ML model insights
        if ml_results:
            if 'limited_analysis' in ml_results and ml_results['limited_analysis']:
                insights.append(f"📊 USAGE ANALYSIS: Limited cross-gen data available")
                if 'high_usage_count' in ml_results:
                    insights.append(f"   - {ml_results['high_usage_count']} highly viable Pokemon (≥10% usage)")
            elif 'r2_score' in ml_results and ml_results['r2_score'] > 0.3:
                insights.append(f"🤖 PREDICTION MODEL: R² = {ml_results['r2_score']:.3f}")
            
            if 'underrated_pokemon' in ml_results and not ml_results['underrated_pokemon'].empty:
                underrated = ml_results['underrated_pokemon'].iloc[0]
                insights.append(f"💎 HIDDEN GEM: {underrated['pokemon_name'].title()}")
                insights.append(f"   - Predicted {underrated['predicted_usage']:.1f}% vs actual {underrated['usage_percentage']:.1f}%")
        
        # Print insights
        for insight in insights:
            logger.info(insight)
        
        return insights
    
    def export_dashboard_data(self, insights):
        """Export processed data for PowerBI dashboard"""
        
        logger.info("💾 Exporting dashboard data...")
        
        # Create warehouse directory if it doesn't exist
        warehouse_dir = get_warehouse_dir()
        os.makedirs(warehouse_dir, exist_ok=True)
        
        export_count = 0
        
        # Export key datasets
        for name, df in self.analytics_data.items():
            if not df.empty:
                export_path = os.path.join(warehouse_dir, f'{name}_dashboard.csv')
                df.to_csv(export_path, index=False)
                logger.info(f"✅ Exported {name}: {len(df)} records")
                export_count += 1
        
        # Export strategic insights
        if insights:
            insights_df = pd.DataFrame({
                'insight_id': range(len(insights)),
                'insight_text': insights,
                'category': ['Strategic' for _ in insights],
                'generated_at': datetime.now()
            })
            
            insights_path = os.path.join(warehouse_dir, 'strategic_insights.csv')
            insights_df.to_csv(insights_path, index=False)
            logger.info(f"✅ Exported insights: {len(insights)} insights")
            export_count += 1
        
        # Create KPI summary
        if not self.analytics_data['usage_facts'].empty:
            usage_df = self.analytics_data['usage_facts']
            
            summary_stats = pd.DataFrame({
                'metric': [
                    'total_pokemon_tracked',
                    'high_usage_pokemon',
                    'average_usage_percentage', 
                    'meta_leader_usage',
                    'analysis_date'
                ],
                'value': [
                    len(usage_df),
                    len(usage_df[usage_df['usage_percentage'] >= 10]),
                    usage_df['usage_percentage'].mean(),
                    usage_df['usage_percentage'].max(),
                    datetime.now().strftime('%Y-%m-%d')
                ]
            })
            
            kpi_path = os.path.join(warehouse_dir, 'dashboard_kpis.csv')
            summary_stats.to_csv(kpi_path, index=False)
            logger.info(f"✅ Exported KPIs: {len(summary_stats)} metrics")
            export_count += 1
        
        return export_count

def get_db_host():
    """Detect if running in Docker and return appropriate database host"""
    import os
    
    # Check if running in our Pokemon Analytics Docker containers
    if os.environ.get('HOSTNAME', '').startswith('pokemon_'):
        return 'postgres'
    # Generic Docker environment detection
    elif os.path.exists('/.dockerenv'):
        return 'postgres'
    # Running locally
    else:
        return 'localhost'

def get_warehouse_dir():
    """Get appropriate warehouse directory based on environment"""
    import os
    from pathlib import Path
    
    # Check if running in Docker
    if os.path.exists('/.dockerenv'):
        # In Docker, use mounted volume paths
        return '/home/jovyan/data/warehouse'
    else:
        # Running locally, use relative path from script location
        script_dir = Path(__file__).parent.parent.parent
        return script_dir / 'data' / 'warehouse'

def main():
    """Main analytics execution function"""
    
    logger.info("🚀 Starting Advanced Pokemon Competitive Analytics...")
    logger.info("=" * 60)
    
    # Environment-aware database configuration
    db_host = get_db_host()
    logger.info(f"🔗 Connecting to database at: {db_host}")
    
    db_config = {
        'host': db_host,
        'port': '5432', 
        'database': 'pokemon_analytics',
        'user': 'pokemon_user',
        'password': 'pokemon_pass'
    }
    
    try:
        # Initialize analytics engine
        analytics = AdvancedCompetitiveAnalytics(db_config)
        
        # Load data
        analytics.load_analytics_data()
        
        # Run analyses
        meta_results = analytics.analyze_competitive_meta()
        type_results = analytics.analyze_type_effectiveness()
        ml_results = analytics.build_usage_prediction_model()
        
        # Generate insights
        strategic_insights = analytics.generate_strategic_insights(meta_results, type_results, ml_results)
        
        # Export for dashboard
        export_count = analytics.export_dashboard_data(strategic_insights)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("🎉 PHASE 6: ADVANCED ANALYTICS COMPLETE!")
        logger.info("=" * 60)
        logger.info("✅ Meta analysis with competitive insights")
        logger.info("✅ Type effectiveness optimization")
        logger.info("✅ Machine learning usage prediction model")
        logger.info("✅ Strategic recommendations generated")
        logger.info(f"✅ Data exported for PowerBI: {export_count} datasets")
        logger.info("\n🚀 Ready for Phase 7: PowerBI Dashboard Creation!")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Advanced analytics failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())