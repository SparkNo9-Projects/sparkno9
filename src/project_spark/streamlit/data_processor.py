"""
Data access and file operations for campaign data processing.
"""

import pandas as pd
import logging
from typing import Dict, Tuple
import validators as streamlit_validators

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CampaignDataProcessor:
    """Main class for processing campaign and naming key data."""
    
    def __init__(self):
        """Initialize the data processor with expected schemas."""
        
        # Define expected columns for campaign data
        self.expected_campaign_columns = {
            # Campaign & Ad Info
            'campaign_name': 'STRING', 'ad_name': 'STRING', 'ad_set_name': 'STRING', 'ad_delivery': 'STRING',
            'starts': 'DATETIME', 'ends': 'DATETIME', 'reporting_starts': 'DATETIME',
            'reporting_ends': 'DATETIME', 'last_significant_edit': 'DATETIME',
            'wave_number': 'INT', 'attribution_setting': 'STRING',
            # Budget & Spend
            'amount_spent_usd': 'FLOAT', 'ad_set_budget': 'FLOAT', 'ad_set_budget_type': 'STRING',
            'bid': 'FLOAT', 'bid_type': 'STRING', 'cpm_usd': 'FLOAT',
            # Core Performance Metrics
            'results': 'INT', 'result_indicator': 'STRING', 'cost_per_result': 'FLOAT',
            'frequency': 'FLOAT', 'reach': 'INT', 'impressions': 'INT',
            'unique_link_clicks': 'INT', 'landing_page_views': 'INT', 'email_signups': 'INT',
            # Essential KPIs (lowercase for consistency with sql_templates.py)
            'kpv_community': 'INT', 'kpv_tool': 'INT', 'kpv_transformation': 'INT',
            'kpv_support': 'INT', 'kpv_nohero': 'INT', 'kpv_inspiration': 'INT',
            'kpv_authentic': 'INT', 'kpv_nextlevel': 'INT', 'kpv_nextchapter': 'INT',
            'kpv_workshop': 'INT', 'kpv_openhouse': 'INT',
            # Lead Generation (lowercase for consistency)
            'lead_openhouse': 'INT', 'lead_workshop': 'INT', 'lead_info': 'INT',
            # Click Events (lowercase for consistency)
            'click_findout': 'INT', 'click_letschat': 'INT', 'click_openhouse': 'INT',
            'click_workshop': 'INT', 'click_info': 'INT',
            # E-commerce Metrics - Add to Cart
            'adds_to_cart': 'INT', 'in_app_adds_to_cart': 'INT', 'website_adds_to_cart': 'INT',
            'offline_adds_to_cart': 'INT', 'meta_add_to_cart': 'INT',
            # E-commerce Metrics - Checkouts
            'checkouts_initiated': 'INT', 'in_app_checkouts': 'INT', 'website_checkouts': 'INT',
            'offline_checkouts': 'INT', 'meta_checkouts': 'INT',
            # E-commerce Metrics - Purchases
            'purchases': 'INT', 'in_app_purchases': 'INT', 'website_purchases': 'INT',
            'offline_purchases': 'INT', 'meta_purchases': 'INT',
            # Registration Metrics
            'registrations_completed': 'INT', 'in_app_registrations': 'INT',
            'website_registrations': 'INT', 'offline_registrations': 'INT',
            # Social Engagement Metrics
            'instagram_profile_visits': 'INT', 'post_comments': 'INT', 'post_reactions': 'INT',
            'post_saves': 'INT', 'post_shares': 'INT', 'post_engagements': 'INT',
            'video_avg_play_time': 'FLOAT'
        }
        
        # Define expected columns for naming key
        self.expected_naming_columns = {
            'ad_set_name': 'STRING',
            'audience': 'STRING',
            'concept': 'STRING',
            'position': 'STRING',
            'ad_descriptor': 'STRING',
            'ad_direction': 'STRING',
            'landing_page': 'STRING'
        }

    def _infer_column_type(self, series: pd.Series) -> str:
        """
        Infer the intended data type of a column by analyzing its values.
        
        Args:
            series: Pandas Series to analyze
            
        Returns:
            Data type as string: 'INT', 'FLOAT', 'DATETIME', or 'STRING'
        """
        # Already typed correctly
        if pd.api.types.is_datetime64_any_dtype(series):
            return 'DATETIME'
        if pd.api.types.is_integer_dtype(series):
            return 'INT'
        if pd.api.types.is_float_dtype(series):
            return 'FLOAT'
        if pd.api.types.is_bool_dtype(series):
            return 'INT'
        
        # For object/string columns, try to infer by converting
        # Drop nulls for analysis
        non_null = series.dropna()
        if len(non_null) == 0:
            return 'STRING'
        
        # Try converting to numeric
        numeric_converted = pd.to_numeric(non_null, errors='coerce')
        numeric_success_rate = numeric_converted.notna().sum() / len(non_null)
        
        # If >80% can be converted to numeric, treat as numeric
        if numeric_success_rate > 0.8:
            # Check if all successful conversions are integers
            successful_numeric = numeric_converted.dropna()
            if len(successful_numeric) > 0:
                is_integer = (successful_numeric == successful_numeric.astype(int)).all()
                return 'INT' if is_integer else 'FLOAT'
        
        # Try converting to datetime
        try:
            datetime_converted = pd.to_datetime(non_null, errors='coerce')
            datetime_success_rate = datetime_converted.notna().sum() / len(non_null)
            if datetime_success_rate > 0.8:
                return 'DATETIME'
        except:
            pass
        
        # Default to string
        return 'STRING'

    def _clean_time_formatted_values(self, df: pd.DataFrame) -> None:
        """
        Clean time-formatted string values (e.g., '0:00:00', '1:23:45') from ALL columns.
        This is applied to any column that should be numeric based on its data.
        Modifies DataFrame in place.
        
        Args:
            df: DataFrame to clean
        """
        # Pattern to match time formats like '0:00:00', '1:23:45', 'HH:MM:SS'
        time_pattern = r'^\d{1,2}:\d{2}:\d{2}$'
        
        total_cleaned = 0
        
        for col in df.columns:
            # Skip if already numeric type
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            # Infer what type this column should be
            inferred_type = self._infer_column_type(df[col])
            
            # Only clean if column should be numeric
            if inferred_type in ['INT', 'FLOAT']:
                # Convert to string to check for time patterns
                col_as_str = df[col].astype(str)
                
                # Find values matching time pattern or other non-numeric patterns
                time_mask = col_as_str.str.match(time_pattern, na=False)
                
                if time_mask.any():
                    count_cleaned = time_mask.sum()
                    total_cleaned += count_cleaned
                    logger.info(f"Column '{col}': Found {count_cleaned} time-formatted values, converting to NaN (inferred type: {inferred_type})")
                    
                    # Replace time-formatted strings with NaN
                    df.loc[time_mask, col] = None
        
        if total_cleaned > 0:
            logger.info(f"✓ Total time-formatted values cleaned: {total_cleaned}")

    def _process_new_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process columns that aren't in the expected schema.
        Infers their types and converts them appropriately.
        
        Args:
            df: DataFrame with potentially new columns
            
        Returns:
            DataFrame with new columns properly typed
        """
        # Find columns not in expected schema
        expected_cols = set(self.expected_campaign_columns.keys())
        new_cols = [col for col in df.columns if col not in expected_cols]
        
        if new_cols:
            logger.info(f"Found {len(new_cols)} new columns not in schema: {new_cols}")
            
            for col in new_cols:
                inferred_type = self._infer_column_type(df[col])
                logger.info(f"  - {col}: inferred type = {inferred_type}")
                
                # Convert based on inferred type
                if inferred_type == 'INT':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif inferred_type == 'FLOAT':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif inferred_type == 'DATETIME':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif inferred_type == 'STRING':
                    df[col] = df[col].astype(str).fillna('')
        
        return df

    def preprocess_campaign_data(self, df: pd.DataFrame, wave_number: int) -> pd.DataFrame:
        """
        Preprocess campaign data DataFrame with robust handling of new columns and data quality issues.
        
        Args:
            df: Raw campaign data DataFrame
            wave_number: Wave number to add to data
            
        Returns:
            Processed campaign DataFrame
        """
        logger.info(f"Preprocessing campaign data with {len(df)} rows")
        
        # Create a copy to avoid modifying original
        processed_df = df.copy()
        
        # Normalize all column names (remove spaces, make lowercase, replace special chars)
        column_mapping = {}
        for col in processed_df.columns:
            normalized_col = streamlit_validators.normalize_column_name(col)
            column_mapping[col] = normalized_col
        
        # Rename columns
        processed_df = processed_df.rename(columns=column_mapping)
        
        logger.info("Step 1: Column normalization complete")
        
        # CRITICAL: Clean time-formatted strings and other problematic values BEFORE type conversion
        # This handles cases like '0:00:00' in numeric columns
        self._clean_time_formatted_values(processed_df)
        
        logger.info("Step 2: Data cleaning complete")
        
        # Add wave number
        processed_df['wave_number'] = wave_number
        
        # Handle ad_name fallback logic - use ad_set_name if ad_name is missing or has null values
        if 'ad_name' not in processed_df.columns and 'ad_set_name' in processed_df.columns:
            logger.info("ad_name column missing, using ad_set_name as fallback")
            processed_df['ad_name'] = processed_df['ad_set_name']
        elif 'ad_name' in processed_df.columns and 'ad_set_name' not in processed_df.columns:
            logger.info("ad_set_name column missing, using ad_name as fallback")
            processed_df['ad_set_name'] = processed_df['ad_name']
        elif 'ad_name' in processed_df.columns and 'ad_set_name' in processed_df.columns:
            # Check for null/empty values in ad_name and fill with ad_set_name
            ad_name_null_mask = processed_df['ad_name'].isnull() | (processed_df['ad_name'].astype(str).str.strip() == '')
            if ad_name_null_mask.any():
                logger.info(f"Found {ad_name_null_mask.sum()} null/empty ad_name values, filling with ad_set_name")
                processed_df.loc[ad_name_null_mask, 'ad_name'] = processed_df.loc[ad_name_null_mask, 'ad_set_name']
            
            # Check for null/empty values in ad_set_name and fill with ad_name
            ad_set_name_null_mask = processed_df['ad_set_name'].isnull() | (processed_df['ad_set_name'].astype(str).str.strip() == '')
            if ad_set_name_null_mask.any():
                logger.info(f"Found {ad_set_name_null_mask.sum()} null/empty ad_set_name values, filling with ad_name")
                processed_df.loc[ad_set_name_null_mask, 'ad_set_name'] = processed_df.loc[ad_set_name_null_mask, 'ad_name']
        
        # Add missing columns with default values
        for col, dtype in self.expected_campaign_columns.items():
            if col not in processed_df.columns:
                if dtype == 'INT':
                    processed_df[col] = None  # Keep as NULL for missing metrics
                elif dtype == 'FLOAT':
                    processed_df[col] = None  # Keep as NULL for missing metrics
                elif dtype == 'STRING':
                    processed_df[col] = ''
                elif dtype == 'DATETIME':
                    processed_df[col] = None
        
        # Convert data types for EXPECTED columns
        for col, dtype in self.expected_campaign_columns.items():
            if col in processed_df.columns:
                if dtype == 'INT':
                    processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')  # Keep NULLs as NULL
                elif dtype == 'FLOAT':
                    processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')  # Keep NULLs as NULL
                elif dtype == 'STRING':
                    processed_df[col] = processed_df[col].astype(str).fillna('')
                elif dtype == 'DATETIME':
                    # Keep as datetime object for proper Snowflake TIMESTAMP_NTZ insertion
                    processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
        
        logger.info("Step 3: Expected columns type conversion complete")
        
        # Process NEW columns not in the expected schema with intelligent type inference
        processed_df = self._process_new_columns(processed_df)
        
        logger.info("Step 4: New columns processed and typed")
        
        # Remove duplicates
        processed_df = processed_df.drop_duplicates()
        
        logger.info(f"✓ Preprocessing complete: {len(processed_df)} rows, {len(processed_df.columns)} columns")
        return processed_df

    def preprocess_naming_data(self, df: pd.DataFrame, wave_number: int) -> pd.DataFrame:
        """
        Preprocess naming key DataFrame.
        
        Args:
            df: Raw naming key DataFrame
            wave_number: Wave number to add to data
            
        Returns:
            Processed naming DataFrame
        """
        logger.info(f"Preprocessing naming data with {len(df)} rows")
        
        # Create a copy to avoid modifying original
        processed_df = df.copy()
        
        # Normalize all column names (remove spaces, make lowercase, replace special chars)
        column_mapping = {}
        for col in processed_df.columns:
            normalized_col = streamlit_validators.normalize_column_name(col)
            column_mapping[col] = normalized_col
        
        # Rename columns
        processed_df = processed_df.rename(columns=column_mapping)
        
        # Add wave number
        processed_df['wave_number'] = wave_number
        
        # Add missing columns with default values
        for col, dtype in self.expected_naming_columns.items():
            if col not in processed_df.columns:
                if dtype == 'INT':
                    processed_df[col] = None  # Keep as NULL for missing values
                elif dtype == 'FLOAT':
                    processed_df[col] = None  # Keep as NULL for missing values
                elif dtype == 'STRING':
                    processed_df[col] = ''
                elif dtype == 'DATETIME':
                    processed_df[col] = None
        
        # Convert data types
        for col, dtype in self.expected_naming_columns.items():
            if col in processed_df.columns:
                if dtype == 'INT':
                    processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')  # Keep NULLs as NULL
                elif dtype == 'FLOAT':
                    processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')  # Keep NULLs as NULL
                elif dtype == 'STRING':
                    processed_df[col] = processed_df[col].astype(str).fillna('')
                elif dtype == 'DATETIME':
                    # Keep as datetime object for proper Snowflake TIMESTAMP_NTZ insertion
                    processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
        
        # Remove duplicates
        processed_df = processed_df.drop_duplicates()
        
        logger.info(f"Preprocessed naming data: {len(processed_df)} rows, {len(processed_df.columns)} columns")
        return processed_df

    def process_files(self, campaign_file_path: str, naming_file_path: str, wave_number: int, client_name: str, platform: str, year: int) -> Dict:
        """
        Process campaign and naming key files.
        
        Args:
            campaign_file_path: Path to campaign data CSV
            naming_file_path: Path to naming key CSV
            wave_number: Wave number for the data
            client_name: Client name (e.g., 'caterpillar')
            platform: Platform name (e.g., 'meta')
            year: Year (e.g., 2024)
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"Starting processing for wave {wave_number}")
        
        try:
            # Load files
            campaign_df = pd.read_csv(campaign_file_path)
            naming_df = pd.read_csv(naming_file_path)
            
            logger.info(f"Loaded campaign data: {len(campaign_df)} rows, {len(campaign_df.columns)} columns")
            logger.info(f"Loaded naming data: {len(naming_df)} rows, {len(naming_df.columns)} columns")
            
            # Validate data (returns errors and warnings)
            campaign_valid, campaign_errors, campaign_warnings = streamlit_validators.validate_campaign_data(campaign_df)
            naming_valid, naming_errors, naming_warnings = streamlit_validators.validate_naming_data(naming_df)
            
            # Combine all warnings
            all_warnings = campaign_warnings + naming_warnings
            
            # Only stop processing for critical errors (empty files, data type issues)
            if not campaign_valid:
                logger.error(f"Campaign data validation failed: {campaign_errors}")
                return {'success': False, 'errors': campaign_errors, 'warnings': all_warnings}
            
            if not naming_valid:
                logger.error(f"Naming data validation failed: {naming_errors}")
                return {'success': False, 'errors': naming_errors, 'warnings': all_warnings}
            
            # Preprocess data
            processed_campaign_df = self.preprocess_campaign_data(campaign_df, wave_number)
            processed_naming_df = self.preprocess_naming_data(naming_df, wave_number)
            
            # Generate quality report
            quality_report = streamlit_validators.generate_data_quality_report(processed_campaign_df, processed_naming_df)
            
            logger.info("Processing completed successfully")

            # Generate dynamic schema name (NEW: without platform)
            # Format: CLIENT_CATERPILLAR_2024 (not CLIENT_CATERPILLAR_META_2024)
            schema_name = f"CLIENT_{client_name.upper()}_{year}"

            return {
                'success': True,
                'campaign_data': processed_campaign_df,
                'naming_data': processed_naming_df,
                'quality_report': quality_report,
                'warnings': all_warnings,
                'schema_name': schema_name,
                'client_name': client_name,
                'platform': platform,
                'year': year,
                'original_campaign_shape': campaign_df.shape,
                'original_naming_shape': naming_df.shape,
                'processed_campaign_shape': processed_campaign_df.shape,
                'processed_naming_shape': processed_naming_df.shape
            }
            
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            return {'success': False, 'errors': [str(e)], 'warnings': []}