"""
CENTRALIZED SERVER-SIDE CLIENT CONFIGURATION TEMPLATE
Single source of truth for all server-side client-specific settings

CUSTOMIZER INSTRUCTIONS:
- Replace Cellular Zone with actual client name
- Replace app9JgRBZC2GNlaKM with client's Airtable base ID
- Replace table IDs with client's actual table IDs
- Set enabled/disabled data sources based on client needs
"""

class ClientConfig:
    """Centralized client configuration for server-side operations"""
    
    # ===== CLIENT INFORMATION =====
    CLIENT_NAME = 'Cellular Zone'
    BUSINESS_NAME = 'Cellular Zone'
    
    # ===== AIRTABLE CONFIGURATION =====
    AIRTABLE_BASE_ID = 'app9JgRBZC2GNlaKM'
    
    # Table IDs - set to None to disable a data source
    TABLE_IDS = {
        'ghl': 'tbl0Er1mMwZO1Pvfj',
        'google_ads': 'tblIhvVihVoghHfVa',
        'pos': None,
        'meta_ads': None,
        'meta_ads_simplified': None,
        'meta_ads_summary': None,
        'meta_ads_performance': None
    }
    
    # ===== DATA SOURCE CONFIGURATION =====
    ENABLED_SOURCES = ['ghl', 'google_ads']
    DISABLED_SOURCES = []
    
    # ===== TABLE MAPPINGS FOR FRESH_TABLES =====
    @classmethod
    def get_fresh_tables(cls):
        """Generate FRESH_TABLES configuration based on client settings"""
        fresh_tables = {}
        
        if cls.is_enabled('ghl'):
            fresh_tables['ghl'] = {
                'id': cls.TABLE_IDS['ghl'],
                'name': f'{cls.CLIENT_NAME} GHL',
                'date_field': 'Date Created',
                'sort_direction': 'desc'
            }
        
        if cls.is_enabled('pos'):
            fresh_tables['pos'] = {
                'id': cls.TABLE_IDS['pos'],
                'name': f'{cls.CLIENT_NAME} POS',
                'date_field': 'Created',
                'sort_direction': 'desc'
            }
        
        if cls.is_enabled('google_ads'):
            fresh_tables['google_ads'] = {
                'id': cls.TABLE_IDS['google_ads'],
                'name': f'{cls.CLIENT_NAME} Google Ads',
                'date_field': 'Date',
                'sort_direction': 'desc'
            }
        
        if cls.is_enabled('meta_ads'):
            fresh_tables['meta_ads'] = {
                'id': cls.TABLE_IDS['meta_ads'],
                'name': f'{cls.CLIENT_NAME} Meta Ads',
                'date_field': 'Reporting ends',
                'sort_direction': 'desc'
            }
        
        if cls.is_enabled('meta_ads_summary'):
            fresh_tables['meta_ads_summary'] = {
                'id': cls.TABLE_IDS['meta_ads_summary'],
                'name': f'{cls.CLIENT_NAME} Meta Ads Summary',
                'date_field': 'Reporting ends',
                'sort_direction': 'desc'
            }
        
        if cls.is_enabled('meta_ads_simplified'):
            fresh_tables['meta_ads_simplified'] = {
                'id': cls.TABLE_IDS['meta_ads_simplified'],
                'name': f'{cls.CLIENT_NAME} Meta Ads Simplified',
                'date_field': 'period',
                'sort_direction': 'desc'
            }
        
        if cls.is_enabled('meta_ads_performance'):
            fresh_tables['meta_ads_performance'] = {
                'id': cls.TABLE_IDS['meta_ads_performance'],
                'name': f'{cls.CLIENT_NAME} Meta Ads Performance',
                'date_field': 'Date',
                'sort_direction': 'desc'
            }
        
        return fresh_tables
    
    # ===== HELPER METHODS =====
    @classmethod
    def is_enabled(cls, data_source):
        """Check if a data source is enabled"""
        return (data_source in cls.ENABLED_SOURCES and 
                cls.TABLE_IDS.get(data_source) is not None and
                cls.TABLE_IDS.get(data_source) != 'null' and
                cls.TABLE_IDS.get(data_source) != '')
    
    @classmethod
    def get_table_id(cls, data_source):
        """Get table ID for a data source"""
        table_id = cls.TABLE_IDS.get(data_source)
        return table_id if table_id and table_id != 'null' and table_id != '' else None
    
    @classmethod
    def get_base_id(cls):
        """Get Airtable base ID"""
        return cls.AIRTABLE_BASE_ID
    
    @classmethod
    def get_client_config_for_frontend(cls):
        """Generate client configuration for frontend use"""
        return {
            "client_info": {
                "client_id": cls.CLIENT_NAME.lower().replace(' ', '_'),
                "business_name": cls.BUSINESS_NAME
            },
            "data_sources": {
                "enabled_sources": cls.ENABLED_SOURCES,
                "disabled_sources": cls.DISABLED_SOURCES
            },
            "tab_configuration": {
                "enabled_tabs": ["overview"] + cls.ENABLED_SOURCES,
                "default_tab": "overview"
            },
            "airtable_configuration": {
                "base_id": cls.AIRTABLE_BASE_ID
            }
        }

# ===== LEGACY COMPATIBILITY =====
# Keep these for backward compatibility with existing code

def get_client_config():
    """Get client configuration (legacy compatibility)"""
    return ClientConfig.get_client_config_for_frontend()

def get_base_id():
    """Get base ID (legacy compatibility)"""
    return ClientConfig.get_base_id()

def get_fresh_tables():
    """Get fresh tables configuration (legacy compatibility)"""
    return ClientConfig.get_fresh_tables()

# Export for easy importing
__all__ = ['ClientConfig', 'get_client_config', 'get_base_id', 'get_fresh_tables']
