-- Pokemon Analytics Database Schema
-- Initial setup for Pokemon competitive analysis

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw data tables (direct from API/scraping)
CREATE TABLE IF NOT EXISTS raw.pokemon (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    height INTEGER,
    weight INTEGER,
    base_experience INTEGER,
    order_id INTEGER,
    is_default BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.pokemon_types (
    pokemon_id INTEGER REFERENCES raw.pokemon(id),
    type_id INTEGER,
    type_name VARCHAR(50),
    slot INTEGER,
    PRIMARY KEY (pokemon_id, slot)
);

CREATE TABLE IF NOT EXISTS raw.pokemon_stats (
    pokemon_id INTEGER REFERENCES raw.pokemon(id),
    stat_name VARCHAR(50),
    base_stat INTEGER,
    effort INTEGER,
    PRIMARY KEY (pokemon_id, stat_name)
);

CREATE TABLE IF NOT EXISTS raw.pokemon_abilities (
    pokemon_id INTEGER REFERENCES raw.pokemon(id),
    ability_id INTEGER,
    ability_name VARCHAR(100),
    is_hidden BOOLEAN DEFAULT false,
    slot INTEGER,
    PRIMARY KEY (pokemon_id, ability_id, slot)
);

CREATE TABLE IF NOT EXISTS raw.pokemon_moves (
    pokemon_id INTEGER REFERENCES raw.pokemon(id),
    move_id INTEGER,
    move_name VARCHAR(100),
    learn_method VARCHAR(50),
    level_learned INTEGER,
    PRIMARY KEY (pokemon_id, move_id, learn_method)
);

-- Staging tables (cleaned and transformed)
CREATE TABLE IF NOT EXISTS staging.pokemon_base (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    height_m DECIMAL(4,2),
    weight_kg DECIMAL(6,2),
    base_experience INTEGER,
    primary_type VARCHAR(50),
    secondary_type VARCHAR(50),
    hp INTEGER,
    attack INTEGER,
    defense INTEGER,
    special_attack INTEGER,
    special_defense INTEGER,
    speed INTEGER,
    total_stats INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Competitive data tables
CREATE TABLE IF NOT EXISTS raw.usage_stats (
    id SERIAL PRIMARY KEY,
    pokemon_name VARCHAR(100),
    format VARCHAR(50),
    usage_percentage DECIMAL(8,4),
    rank_position INTEGER,
    month_year VARCHAR(7),
    raw_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.team_compositions (
    id SERIAL PRIMARY KEY,
    team_hash VARCHAR(64) UNIQUE,
    pokemon_1 VARCHAR(100),
    pokemon_2 VARCHAR(100),
    pokemon_3 VARCHAR(100),
    pokemon_4 VARCHAR(100),
    pokemon_5 VARCHAR(100),
    pokemon_6 VARCHAR(100),
    format VARCHAR(50),
    rating INTEGER,
    wins INTEGER,
    losses INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Analytics tables (final dimensional model)
CREATE TABLE IF NOT EXISTS analytics.dim_pokemon (
    pokemon_key SERIAL PRIMARY KEY,
    pokemon_id INTEGER UNIQUE,
    name VARCHAR(100) NOT NULL,
    primary_type VARCHAR(50),
    secondary_type VARCHAR(50),
    type_combination VARCHAR(100),
    height_m DECIMAL(4,2),
    weight_kg DECIMAL(6,2),
    base_stats_total INTEGER,
    hp INTEGER,
    attack INTEGER,
    defense INTEGER,
    special_attack INTEGER,
    special_defense INTEGER,
    speed INTEGER,
    tier VARCHAR(20),
    is_legendary BOOLEAN DEFAULT false,
    generation INTEGER
);

CREATE TABLE IF NOT EXISTS analytics.dim_time (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    season VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS analytics.fact_usage (
    usage_key SERIAL PRIMARY KEY,
    pokemon_key INTEGER REFERENCES analytics.dim_pokemon(pokemon_key),
    date_key INTEGER REFERENCES analytics.dim_time(date_key),
    format VARCHAR(50),
    usage_percentage DECIMAL(8,4),
    rank_position INTEGER,
    raw_count BIGINT,
    viability_score DECIMAL(5,2)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_pokemon_name ON raw.pokemon(name);
CREATE INDEX IF NOT EXISTS idx_pokemon_types_pokemon_id ON raw.pokemon_types(pokemon_id);
CREATE INDEX IF NOT EXISTS idx_usage_stats_pokemon_format ON raw.usage_stats(pokemon_name, format);
CREATE INDEX IF NOT EXISTS idx_usage_stats_month ON raw.usage_stats(month_year);
CREATE INDEX IF NOT EXISTS idx_fact_usage_pokemon ON analytics.fact_usage(pokemon_key);
CREATE INDEX IF NOT EXISTS idx_fact_usage_date ON analytics.fact_usage(date_key);

-- Insert some time dimension data
INSERT INTO analytics.dim_time (date_key, full_date, year, month, month_name, quarter, season)
SELECT 
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER) as date_key,
    d as full_date,
    EXTRACT(YEAR FROM d) as year,
    EXTRACT(MONTH FROM d) as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(QUARTER FROM d) as quarter,
    CASE 
        WHEN EXTRACT(MONTH FROM d) IN (12,1,2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM d) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6,7,8) THEN 'Summer'
        ELSE 'Fall'
    END as season
FROM generate_series('2020-01-01'::date, '2025-12-31'::date, '1 day') d
ON CONFLICT (date_key) DO NOTHING;