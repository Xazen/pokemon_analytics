#!/usr/bin/env python3
"""
Docker Utilities for Pokemon Analytics

Provides environment detection and configuration utilities for scripts
that need to work both locally and in Docker containers.
"""

import os
from typing import Dict, Union
from pathlib import Path


def detect_environment() -> str:
    """
    Detect if running in Docker environment
    
    Returns:
        'docker' if running in container, 'local' if running locally
    """
    # Check if running in our Pokemon Analytics Docker containers
    if os.environ.get('HOSTNAME', '').startswith('pokemon_'):
        return 'docker'
    
    # Generic Docker environment detection
    if os.path.exists('/.dockerenv'):
        return 'docker'
    
    # Check for common Docker container indicators
    if os.path.exists('/proc/1/cgroup'):
        try:
            with open('/proc/1/cgroup', 'r') as f:
                if 'docker' in f.read():
                    return 'docker'
        except:
            pass
    
    return 'local'


def get_database_config() -> Dict[str, str]:
    """
    Get database configuration based on environment
    
    Returns:
        Database configuration dictionary
    """
    env = detect_environment()
    
    # In Docker, use container networking
    if env == 'docker':
        host = 'postgres'
    else:
        host = 'localhost'
    
    return {
        'host': host,
        'port': '5432',
        'database': 'pokemon_analytics', 
        'user': 'pokemon_user',
        'password': 'pokemon_pass'
    }


def get_data_directory(subdir: str = '') -> Union[str, Path]:
    """
    Get appropriate data directory based on environment
    
    Args:
        subdir: Subdirectory within data folder (e.g., 'warehouse', 'raw')
    
    Returns:
        Path to data directory
    """
    env = detect_environment()
    
    if env == 'docker':
        # In Docker containers, use mounted volume paths
        base_path = Path('/home/jovyan/data')
        if not base_path.exists():
            # Alternative Docker mount points
            base_path = Path('/app/data')
    else:
        # Running locally, use relative path from project root
        script_dir = Path(__file__).parent.parent.parent
        base_path = script_dir / 'data'
    
    if subdir:
        return base_path / subdir
    return base_path


def get_src_directory() -> Path:
    """
    Get source code directory path
    
    Returns:
        Path to src directory
    """
    env = detect_environment()
    
    if env == 'docker':
        # In Docker containers
        src_path = Path('/home/jovyan/src')
        if not src_path.exists():
            src_path = Path('/app/src')
    else:
        # Running locally
        src_path = Path(__file__).parent.parent
    
    return src_path


def log_environment_info():
    """Log current environment information for debugging"""
    import logging
    
    env = detect_environment()
    db_config = get_database_config()
    data_dir = get_data_directory()
    
    logging.info(f"🌐 Environment: {env}")
    logging.info(f"🔗 Database host: {db_config['host']}")
    logging.info(f"📁 Data directory: {data_dir}")
    
    # Additional debug info
    if env == 'docker':
        hostname = os.environ.get('HOSTNAME', 'unknown')
        logging.info(f"🐳 Container hostname: {hostname}")


if __name__ == "__main__":
    """Test the utilities"""
    import logging
    
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("🧪 Testing Docker utilities...")
    log_environment_info()
    
    print(f"\nDatabase config: {get_database_config()}")
    print(f"Data directory: {get_data_directory()}")
    print(f"Warehouse directory: {get_data_directory('warehouse')}")
    print(f"Source directory: {get_src_directory()}")