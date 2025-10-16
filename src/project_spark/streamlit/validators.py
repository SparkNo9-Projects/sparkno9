"""
Utility functions for campaign data preprocessing and validation.
"""

import pandas as pd
import re
from typing import Tuple, List, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_column_name(col_name: str) -> str:
    """
    Normalize column name: replace spaces with underscores, remove other symbols, make lowercase.
    
    Args:
        col_name: Original column name
        
    Returns:
        Normalized column name
    """
    if pd.isna(col_name):
        return ''
    
    # Replace spaces with underscores, remove other symbols, keep only letters/numbers/underscores, make lowercase
    normalized = re.sub(r'[^a-zA-Z0-9 ]', '', str(col_name).strip())
    normalized = normalized.replace(' ', '_').lower()
    # Remove multiple consecutive underscores
    normalized = re.sub(r'_+', '_', normalized)
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    return normalized


def validate_campaign_data(df: pd.DataFrame) -> Tuple[bool, List[str], List[str]]:
    """
    Validate campaign data DataFrame.

    Args:
        df: Campaign data DataFrame

    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    errors = []
    warnings = []

    # Check if DataFrame is empty
    if df.empty:
        errors.append("Campaign data file is empty")
        return False, errors, warnings

    # Get normalized column names
    normalized_columns = [normalize_column_name(col) for col in df.columns]

    # All expected columns for campaign data (from data_access.py expected_campaign_columns)
    expected_columns = [
        'campaign_name', 'ad_name', 'ad_set_name', 'ad_delivery',
        'starts', 'ends', 'reporting_starts', 'reporting_ends', 'last_significant_edit',
        'wave_number', 'attribution_setting',
        'amount_spent_usd', 'ad_set_budget', 'ad_set_budget_type',
        'bid', 'bid_type', 'cpm_usd',
        'results', 'result_indicator', 'cost_per_result',
        'frequency', 'reach', 'impressions',
        'unique_link_clicks', 'landing_page_views', 'email_signups',
        'kpv_community', 'kpv_tool', 'kpv_transformation', 'kpv_support',
        'kpv_nohero', 'kpv_inspiration', 'kpv_authentic', 'kpv_nextlevel',
        'kpv_nextchapter', 'kpv_workshop', 'kpv_openhouse',
        'lead_openhouse', 'lead_workshop', 'lead_info',
        'click_findout', 'click_letschat', 'click_openhouse',
        'click_workshop', 'click_info',
        'adds_to_cart', 'in_app_adds_to_cart', 'website_adds_to_cart',
        'offline_adds_to_cart', 'meta_add_to_cart',
        'checkouts_initiated', 'in_app_checkouts', 'website_checkouts',
        'offline_checkouts', 'meta_checkouts',
        'purchases', 'in_app_purchases', 'website_purchases',
        'offline_purchases', 'meta_purchases',
        'registrations_completed', 'in_app_registrations',
        'website_registrations', 'offline_registrations',
        'instagram_profile_visits', 'post_comments', 'post_reactions',
        'post_saves', 'post_shares', 'post_engagements',
        'video_avg_play_time'
    ]

    # Check for critical required columns (must have at least one of ad_name or ad_set_name)
    critical_columns = ['ad_name', 'ad_set_name']
    missing_critical = []

    # Check if we have at least one of the critical columns
    has_ad_name = 'ad_name' in normalized_columns
    has_ad_set_name = 'ad_set_name' in normalized_columns
    
    if not has_ad_name and not has_ad_set_name:
        missing_critical = ['ad_name', 'ad_set_name']
        errors.append(f"Missing critical columns (required): {missing_critical} - at least one of ad_name or ad_set_name must be present")
    elif not has_ad_name:
        warnings.append("ad_name column missing - will use ad_set_name as fallback")
    elif not has_ad_set_name:
        warnings.append("ad_set_name column missing - will use ad_name as fallback")

    # Check for missing optional columns - warn only
    missing_optional = []
    for exp_col in expected_columns:
        if exp_col not in normalized_columns and exp_col not in critical_columns:
            missing_optional.append(exp_col)

    if missing_optional:
        warnings.append(f"Missing optional campaign columns (will be filled with NULL): {', '.join(missing_optional[:10])}" +
                       (f" and {len(missing_optional) - 10} more" if len(missing_optional) > 10 else ""))

    # Check data types for key columns (using original column names)
    type_errors = []
    numeric_cols_to_check = ['Impressions', 'Results', 'Reach']
    for col in numeric_cols_to_check:
        if col in df.columns:
            try:
                pd.to_numeric(df[col], errors='coerce')
            except:
                type_errors.append(f"Cannot convert {col} to numeric")

    if type_errors:
        errors.extend(type_errors)

    return len(errors) == 0, errors, warnings


def validate_naming_data(df: pd.DataFrame) -> Tuple[bool, List[str], List[str]]:
    """
    Validate naming key DataFrame.

    Args:
        df: Naming key DataFrame

    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    errors = []
    warnings = []

    # Check if DataFrame is empty
    if df.empty:
        errors.append("Naming key file is empty")
        return False, errors, warnings

    # All expected naming key columns (from data_access.py expected_naming_columns)
    expected_naming_columns = [
        'ad_set_name',
        'audience',
        'concept',
        'position',
        'ad_descriptor',
        'ad_direction',
        'landing_page'
    ]

    # Check for normalized columns
    normalized_columns = [normalize_column_name(col) for col in df.columns]

    # Check for critical required column (primary key)
    if 'ad_set_name' not in normalized_columns:
        errors.append("Missing critical column: 'ad_set_name' (required as primary key)")

    # Check for missing optional columns - warn only
    missing_optional = []
    for exp_col in expected_naming_columns:
        if exp_col not in normalized_columns and exp_col != 'ad_set_name':
            missing_optional.append(exp_col)

    if missing_optional:
        warnings.append(f"Missing optional naming columns (will be filled with NULL): {', '.join(missing_optional)}")

    return len(errors) == 0, errors, warnings


def generate_data_quality_report(campaign_df: pd.DataFrame, naming_df: pd.DataFrame) -> Dict:
    """
    Generate data quality report.
    
    Args:
        campaign_df: Processed campaign DataFrame
        naming_df: Processed naming DataFrame
        
    Returns:
        Data quality report dictionary
    """
    report = {
        'campaign_data': {
            'total_rows': len(campaign_df),
            'total_columns': len(campaign_df.columns),
            'missing_values': campaign_df.isnull().sum().to_dict(),
            'duplicate_rows': campaign_df.duplicated().sum(),
            'data_types': campaign_df.dtypes.to_dict()
        },
        'naming_data': {
            'total_rows': len(naming_df),
            'total_columns': len(naming_df.columns),
            'missing_values': naming_df.isnull().sum().to_dict(),
            'duplicate_rows': naming_df.duplicated().sum(),
            'data_types': naming_df.dtypes.to_dict()
        }
    }
    
    return report
