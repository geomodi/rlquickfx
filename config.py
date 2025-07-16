"""
Configuration management for Analytics Dashboard Server
Centralizes all configuration settings and table mappings
"""
import os
from datetime import timedelta

class Config:
    """Base configuration class with common settings"""
    
    # API Configuration
    CLAUDE_API_URL = 'https://api.anthropic.com/v1/messages'
    AIRTABLE_BASE_URL = 'https://api.airtable.com/v0'
    
    # Server Configuration
    HOST = '127.0.0.1'
    PORT = 8000
    
    # Request Timeouts (in seconds)
    CLAUDE_API_TIMEOUT = 30
    AIRTABLE_API_TIMEOUT = 15
    
    # Pagination Settings
    MAX_RECORDS_PER_REQUEST = 100  # Airtable API limit
    MAX_TOTAL_RECORDS = 10000      # Safety limit for large requests
    MAX_PAGINATION_PAGES = 50      # Prevent infinite loops
    
    # Logging Configuration
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
    LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    LOG_FILE_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT = 5
    
    # Security Settings
    CORS_ORIGINS = ['http://localhost:3000', 'http://localhost:8000', 'http://127.0.0.1:8000']
    
    # Table Mappings - Fresh Tables (Primary)
    # Now dynamically generated from centralized client configuration
    try:
        from server_config import ClientConfig
        FRESH_TABLES = ClientConfig.get_fresh_tables()
    except ImportError:
        # Fallback to template placeholders if server-config.py not found
        FRESH_TABLES = {
            'ghl': {
                'id': 'CLIENT_GHL_TABLE_ID',
                'name': 'CLIENT_NAME GHL',
                'date_field': 'Date Created',
                'sort_direction': 'desc'
            },
            'pos': {
                'id': 'CLIENT_POS_TABLE_ID',
                'name': 'CLIENT_NAME POS',
                'date_field': 'Created',
                'sort_direction': 'desc'
            },
            'meta_ads': {
                'id': 'CLIENT_META_ADS_TABLE_ID',
                'name': 'CLIENT_NAME Meta Ads',
                'date_field': 'Reporting ends',
                'sort_direction': 'desc'
            },
            'meta_ads_summary': {
                'id': 'CLIENT_META_ADS_SUMMARY_TABLE_ID',
                'name': 'CLIENT_NAME Meta Ads Summary',
                'date_field': 'Reporting ends',
                'sort_direction': 'desc'
            },
            'meta_ads_simplified': {
                'id': 'CLIENT_META_ADS_SIMPLIFIED_TABLE_ID',
                'name': 'CLIENT_NAME Meta Ads Simplified',
                'date_field': 'period',
                'sort_direction': 'desc'
            },
            'google_ads': {
                'id': 'CLIENT_GOOGLE_ADS_TABLE_ID',
                'name': 'CLIENT_NAME Google Ads',
                'date_field': 'Date',
                'sort_direction': 'desc'
            }
        }
    
    # Legacy Table Mappings - ELIMINATED for centralized configuration
    # All table configurations now come from server-config.py
    LEGACY_TABLES = {
        # Legacy tables completely disabled - using only centralized FRESH_TABLES
    }
    
    @classmethod
    def get_table_config(cls, table_id):
        """
        Get table configuration by table ID.
        Returns table config dict or None if not found.
        """
        # Check fresh tables first
        for table_type, config in cls.FRESH_TABLES.items():
            if config['id'] == table_id:
                return {**config, 'type': table_type, 'is_legacy': False}
        
        # Legacy tables disabled - only using centralized FRESH_TABLES
        # (Legacy table checking removed)
        
        return None
    
    @classmethod
    def get_all_table_ids(cls):
        """Get all known table IDs (only fresh tables for centralized config)"""
        fresh_ids = [config['id'] for config in cls.FRESH_TABLES.values()]
        return fresh_ids

class DevelopmentConfig(Config):
    """Development-specific configuration"""
    DEBUG = True
    LOG_LEVEL = 'DEBUG'
    
    # More verbose logging in development
    LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)-15s | %(funcName)-20s | %(message)s'

class ProductionConfig(Config):
    """Production-specific configuration"""
    DEBUG = False
    LOG_LEVEL = 'INFO'
    
    # Production optimizations
    MAX_TOTAL_RECORDS = 5000  # Lower limit for production
    
    # Stricter CORS in production
    CORS_ORIGINS = ['http://localhost:8000', 'http://127.0.0.1:8000']

class TestConfig(Config):
    """Test-specific configuration"""
    DEBUG = True
    LOG_LEVEL = 'WARNING'  # Less verbose during tests
    
    # Test limits
    MAX_TOTAL_RECORDS = 100
    MAX_PAGINATION_PAGES = 5

# Configuration factory
def get_config():
    """
    Get the appropriate configuration based on environment.
    Returns the config class to use.
    """
    env = os.getenv('FLASK_ENV', 'production').lower()
    
    if env == 'development':
        return DevelopmentConfig
    elif env == 'testing':
        return TestConfig
    else:
        return ProductionConfig

# Export the active configuration
AppConfig = get_config()
