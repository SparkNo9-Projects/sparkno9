"""
Constants and configuration for the Streamlit Campaign Data Processor.
Centralized location for all constants to avoid magic numbers and duplicate definitions.
"""

from typing import List

# ============================================================================
# FILE UPLOAD CONFIGURATION
# ============================================================================

VALID_PLATFORMS: List[str] = [
    "meta",
    "linkedin",
    "facebook",
    "instagram"
]

MAX_FILE_SIZE_MB: int = 50
ALLOWED_FILE_TYPES: List[str] = ['csv']
MAX_WAVE_NUMBER: int = 10
MIN_WAVE_NUMBER: int = 1
MIN_PROJECT_YEAR: int = 2000
MAX_PROJECT_YEAR: int = 2090

# ============================================================================
# SCHEMA CONFIGURATION
# ============================================================================

# New schema structure: One schema per client/year, platform-prefixed tables
# Example: CLIENT_CATERPILLAR_2024 with tables META_NAMING_KEYS, LINKEDIN_NAMING_KEYS, etc.
SCHEMA_NAME_TEMPLATE: str = "CLIENT_{client}_{year}"
TABLE_PREFIX_TEMPLATE: str = "{platform}_"

# Base table names (without platform prefix)
BASE_TABLE_NAMES = [
    "NAMING_KEYS",
    "PROCESSED_CAMPAIGN_DATA",
    "PROCESSING_LOG"
]

# ============================================================================
# EXPECTED CAMPAIGN DATA COLUMNS
# ============================================================================

EXPECTED_CAMPAIGN_COLUMNS: List[str] = [
    # Campaign & Ad Info
    'campaign_name',
    'ad_name',
    'ad_set_name',
    'ad_delivery',
    'starts',
    'ends',
    'reporting_starts',
    'reporting_ends',
    'last_significant_edit',
    'wave_number',
    'attribution_setting',

    # Budget & Spend
    'amount_spent_usd',
    'ad_set_budget',
    'ad_set_budget_type',
    'bid',
    'bid_type',
    'cpm_usd',

    # Core Performance Metrics
    'results',
    'result_indicator',
    'cost_per_result',
    'frequency',
    'reach',
    'impressions',
    'unique_link_clicks',
    'landing_page_views',
    'email_signups',

    # Essential KPIs
    'kpv_community',
    'kpv_tool',
    'kpv_transformation',
    'kpv_support',
    'kpv_nohero',
    'kpv_inspiration',
    'kpv_authentic',
    'kpv_nextlevel',
    'kpv_nextchapter',
    'kpv_workshop',
    'kpv_openhouse',

    # Lead Generation
    'lead_openhouse',
    'lead_workshop',
    'lead_info',

    # Click Events
    'click_findout',
    'click_letschat',
    'click_openhouse',
    'click_workshop',
    'click_info',

    # E-commerce Metrics - Add to Cart
    'adds_to_cart',
    'in_app_adds_to_cart',
    'website_adds_to_cart',
    'offline_adds_to_cart',
    'meta_add_to_cart',

    # E-commerce Metrics - Checkouts
    'checkouts_initiated',
    'in_app_checkouts',
    'website_checkouts',
    'offline_checkouts',
    'meta_checkouts',

    # E-commerce Metrics - Purchases
    'purchases',
    'in_app_purchases',
    'website_purchases',
    'offline_purchases',
    'meta_purchases',

    # Registration Metrics
    'registrations_completed',
    'in_app_registrations',
    'website_registrations',
    'offline_registrations',

    # Social Engagement Metrics
    'instagram_profile_visits',
    'post_comments',
    'post_reactions',
    'post_saves',
    'post_shares',
    'post_engagements',
    'video_avg_play_time'
]

# ============================================================================
# EXPECTED NAMING KEY COLUMNS
# ============================================================================

EXPECTED_NAMING_COLUMNS: List[str] = [
    'ad_set_name',
    'audience',
    'concept',
    'position',
    'ad_descriptor',
    'ad_direction',
    'landing_page'
]

# ============================================================================
# CRITICAL COLUMNS (Must be present)
# ============================================================================

CRITICAL_CAMPAIGN_COLUMNS: List[str] = [
    'ad_name',  # or ad_set_name
    'ad_set_name'
]

CRITICAL_NAMING_COLUMNS: List[str] = [
    'ad_set_name'  # Primary key
]

# ============================================================================
# DATA TYPE DEFINITIONS
# ============================================================================

CAMPAIGN_COLUMN_TYPES = {
    # Campaign & Ad Info
    'campaign_name': 'STRING',
    'ad_name': 'STRING',
    'ad_set_name': 'STRING',
    'ad_delivery': 'STRING',
    'starts': 'DATETIME',
    'ends': 'DATETIME',
    'reporting_starts': 'DATETIME',
    'reporting_ends': 'DATETIME',
    'last_significant_edit': 'DATETIME',
    'wave_number': 'INT',
    'attribution_setting': 'STRING',

    # Budget & Spend
    'amount_spent_usd': 'FLOAT',
    'ad_set_budget': 'FLOAT',
    'ad_set_budget_type': 'STRING',
    'bid': 'FLOAT',
    'bid_type': 'STRING',
    'cpm_usd': 'FLOAT',

    # Core Performance Metrics
    'results': 'INT',
    'result_indicator': 'STRING',
    'cost_per_result': 'FLOAT',
    'frequency': 'FLOAT',
    'reach': 'INT',
    'impressions': 'INT',
    'unique_link_clicks': 'INT',
    'landing_page_views': 'INT',
    'email_signups': 'INT',

    # Essential KPIs
    'kpv_community': 'INT',
    'kpv_tool': 'INT',
    'kpv_transformation': 'INT',
    'kpv_support': 'INT',
    'kpv_nohero': 'INT',
    'kpv_inspiration': 'INT',
    'kpv_authentic': 'INT',
    'kpv_nextlevel': 'INT',
    'kpv_nextchapter': 'INT',
    'kpv_workshop': 'INT',
    'kpv_openhouse': 'INT',

    # Lead Generation
    'lead_openhouse': 'INT',
    'lead_workshop': 'INT',
    'lead_info': 'INT',

    # Click Events
    'click_findout': 'INT',
    'click_letschat': 'INT',
    'click_openhouse': 'INT',
    'click_workshop': 'INT',
    'click_info': 'INT',

    # E-commerce Metrics
    'adds_to_cart': 'INT',
    'in_app_adds_to_cart': 'INT',
    'website_adds_to_cart': 'INT',
    'offline_adds_to_cart': 'INT',
    'meta_add_to_cart': 'INT',
    'checkouts_initiated': 'INT',
    'in_app_checkouts': 'INT',
    'website_checkouts': 'INT',
    'offline_checkouts': 'INT',
    'meta_checkouts': 'INT',
    'purchases': 'INT',
    'in_app_purchases': 'INT',
    'website_purchases': 'INT',
    'offline_purchases': 'INT',
    'meta_purchases': 'INT',

    # Registration Metrics
    'registrations_completed': 'INT',
    'in_app_registrations': 'INT',
    'website_registrations': 'INT',
    'offline_registrations': 'INT',

    # Social Engagement Metrics
    'instagram_profile_visits': 'INT',
    'post_comments': 'INT',
    'post_reactions': 'INT',
    'post_saves': 'INT',
    'post_shares': 'INT',
    'post_engagements': 'INT',
    'video_avg_play_time': 'FLOAT'
}

NAMING_COLUMN_TYPES = {
    'ad_set_name': 'STRING',
    'audience': 'STRING',
    'concept': 'STRING',
    'position': 'STRING',
    'ad_descriptor': 'STRING',
    'ad_direction': 'STRING',
    'landing_page': 'STRING'
}

# ============================================================================
# UI CONFIGURATION
# ============================================================================

APP_TITLE: str = "Spark No. 9 - Campaign Data Processor"
APP_ICON: str = "🚀"
DEFAULT_CLIENT: str = ""
DEFAULT_PLATFORM: str = ""
DEFAULT_WAVE: int = 1
DEFAULT_YEAR: int = 2024

# ============================================================================
# HELPER FUNCTIONS FOR SCHEMA AND TABLE NAMING
# ============================================================================

def get_schema_name(client_name: str, year: int) -> str:
    """
    Generate schema name from client and year.

    Args:
        client_name: Client name (e.g., 'caterpillar')
        year: Year (e.g., 2024)

    Returns:
        Schema name: CLIENT_{CLIENT}_{YEAR}
        Example: CLIENT_CATERPILLAR_2024
    """
    return f"CLIENT_{client_name.upper()}_{year}"


def get_table_name(platform: str, table_base: str) -> str:
    """
    Generate platform-prefixed table name.

    Args:
        platform: Platform name (e.g., 'meta', 'linkedin')
        table_base: Base table name (e.g., 'NAMING_KEYS')

    Returns:
        Prefixed table name: {PLATFORM}_{TABLE}
        Example: META_NAMING_KEYS
    """
    return f"{platform.upper()}_{table_base}"


def get_full_table_name(client_name: str, year: int, platform: str, table_base: str) -> str:
    """
    Generate fully qualified table name.

    Args:
        client_name: Client name (e.g., 'caterpillar')
        year: Year (e.g., 2024)
        platform: Platform name (e.g., 'meta')
        table_base: Base table name (e.g., 'NAMING_KEYS')

    Returns:
        Fully qualified table name: {SCHEMA}.{PLATFORM}_{TABLE}
        Example: CLIENT_CATERPILLAR_2024.META_NAMING_KEYS
    """
    schema = get_schema_name(client_name, year)
    table = get_table_name(platform, table_base)
    return f"{schema}.{table}"


def get_stage_name(client_name: str, year: int) -> str:
    """
    Generate stage name for schema.

    Args:
        client_name: Client name (e.g., 'caterpillar')
        year: Year (e.g., 2024)

    Returns:
        Stage name: CLIENT_{CLIENT}_{YEAR}_STAGE
        Example: CLIENT_CATERPILLAR_2024_STAGE
    """
    schema = get_schema_name(client_name, year)
    return f"{schema}_STAGE"


def get_view_name(platform: str, view_base: str = "AUDIENCE_AD_DESCRIPTOR_DATA") -> str:
    """
    Generate platform-prefixed view name.

    Args:
        platform: Platform name (e.g., 'meta')
        view_base: Base view name

    Returns:
        Prefixed view name: {PLATFORM}_{VIEW}
        Example: META_AUDIENCE_AD_DESCRIPTOR_DATA
    """
    return f"{platform.upper()}_{view_base}"
