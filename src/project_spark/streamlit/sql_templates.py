"""
SQL statement generation for campaign data tables.
"""

from typing import Dict

def generate_schema_creation_statements(schema_name: str, platform: str) -> Dict[str, str]:
    """
    Generate all table creation statements for a specific platform.

    NEW STRUCTURE: One schema per client/year, platform-prefixed tables.
    Example: CLIENT_CATERPILLAR_2024 with META_NAMING_KEYS, LINKEDIN_NAMING_KEYS, etc.

    PROCESSING_LOG: Single table per schema (not platform-prefixed) with platform column.

    This is the SINGLE SOURCE OF TRUTH for all table creation SQL.
    All other components should reference this function to avoid duplication.

    Args:
        schema_name: Target schema name (e.g., 'CLIENT_CATERPILLAR_2024')
        platform: Platform name (e.g., 'meta', 'linkedin')

    Returns:
        Dictionary with statement types as keys and SQL statements as values
    """

    # Generate platform-prefixed table names (except processing_log which is shared)
    prefix = platform.upper()
    naming_keys_table = f"{prefix}_NAMING_KEYS"
    campaign_data_table = f"{prefix}_PROCESSED_CAMPAIGN_DATA"
    processing_log_table = "PROCESSING_LOG"  # Single shared table for all platforms
    stage_name = f"{schema_name}_STAGE"

    statements = {
        'create_schema': f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
        
        'create_stage': f"""
            CREATE STAGE IF NOT EXISTS {schema_name}.{stage_name}
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """.strip(),

        'create_naming_keys_table': f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{naming_keys_table} (
                -- Wave identification
                wave_number INT,

                -- Ad Set Information (Primary Key)
                ad_set_name STRING PRIMARY KEY,
                audience STRING,
                concept STRING,
                position STRING,
                ad_descriptor STRING,
                ad_direction STRING,
                landing_page STRING,

                -- Processing metadata
                upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """.strip(),
            
        'create_processed_campaign_data_table': f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{campaign_data_table} (
                -- Campaign & Ad Info (Primary Key)
                campaign_name STRING,
                ad_name STRING PRIMARY KEY,
                ad_set_name STRING,
                ad_delivery STRING,
                starts DATETIME,
                ends DATETIME,
                reporting_starts DATETIME,
                reporting_ends DATETIME,
                last_significant_edit DATETIME,
                wave_number INT,
                attribution_setting STRING,

                -- Budget & Spend
                amount_spent_usd FLOAT,
                ad_set_budget FLOAT,
                ad_set_budget_type STRING,
                bid FLOAT,
                bid_type STRING,
                cpm_usd FLOAT,

                -- Core Performance Metrics
                results INT,
                result_indicator STRING,
                cost_per_result FLOAT,
                frequency FLOAT,
                reach INT,
                impressions INT,
                unique_link_clicks INT,
                landing_page_views INT,
                email_signups INT,

                -- Essential KPIs (lowercase for consistency)
                kpv_community INT,
                kpv_tool INT,
                kpv_transformation INT,
                kpv_support INT,
                kpv_nohero INT,
                kpv_inspiration INT,
                kpv_authentic INT,
                kpv_nextlevel INT,
                kpv_nextchapter INT,
                kpv_workshop INT,
                kpv_openhouse INT,

                -- Lead Generation (lowercase for consistency)
                lead_openhouse INT,
                lead_workshop INT,
                lead_info INT,

                -- Click Events (lowercase for consistency)
                click_findout INT,
                click_letschat INT,
                click_openhouse INT,
                click_workshop INT,
                click_info INT,

                -- E-commerce Metrics - Add to Cart
                adds_to_cart INT,
                in_app_adds_to_cart INT,
                website_adds_to_cart INT,
                offline_adds_to_cart INT,
                meta_add_to_cart INT,

                -- E-commerce Metrics - Checkouts
                checkouts_initiated INT,
                in_app_checkouts INT,
                website_checkouts INT,
                offline_checkouts INT,
                meta_checkouts INT,

                -- E-commerce Metrics - Purchases
                purchases INT,
                in_app_purchases INT,
                website_purchases INT,
                offline_purchases INT,
                meta_purchases INT,

                -- Registration Metrics
                registrations_completed INT,
                in_app_registrations INT,
                website_registrations INT,
                offline_registrations INT,

                -- Social Engagement Metrics
                instagram_profile_visits INT,
                post_comments INT,
                post_reactions INT,
                post_saves INT,
                post_shares INT,
                post_engagements INT,
                video_avg_play_time FLOAT,

                -- Processing metadata
                upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

                -- Foreign Key Constraint
                FOREIGN KEY (ad_set_name) REFERENCES {schema_name}.{naming_keys_table}(ad_set_name)
            )
            """.strip(),
        
        'create_processing_log_table': f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{processing_log_table} (
                -- Primary key
                log_id INTEGER AUTOINCREMENT PRIMARY KEY,
                
                -- Wave identification
                wave_number INT,
                
                -- Processing metadata
                processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                status STRING,
                records_processed INT,
                errors_count INT,
                warnings_count INT,
                processing_time_seconds FLOAT,
                
                -- Client information
                client_name STRING,
                platform STRING,
                year INT
            )
            """.strip()
    }
    
    return statements


def generate_view_creation_statement(schema_name: str, platform: str) -> str:
    """
    Generate CREATE VIEW statement for AUDIENCE_AD_DESCRIPTOR_DATA view.
    
    This view joins ALL columns from both NAMING_KEYS and PROCESSED_CAMPAIGN_DATA tables.
    Single comprehensive view suitable for all use cases: analysis, reporting, and dashboards.

    NEW STRUCTURE: Platform-prefixed views for clearer organization.
    Example: CLIENT_CATERPILLAR_2024.META_AUDIENCE_AD_DESCRIPTOR_DATA

    This is the SINGLE SOURCE OF TRUTH for the view definition.
    All other components should reference this function to avoid duplication and ensure consistency.

    Args:
        schema_name: Target schema name (e.g., 'CLIENT_CATERPILLAR_2024')
        platform: Platform name (e.g., 'meta', 'linkedin')

    Returns:
        CREATE VIEW SQL statement with all columns
    """
    # Generate platform-prefixed table and view names
    prefix = platform.upper()
    view_name = f"{prefix}_AUDIENCE_AD_DESCRIPTOR_DATA"
    campaign_data_table = f"{prefix}_PROCESSED_CAMPAIGN_DATA"
    naming_keys_table = f"{prefix}_NAMING_KEYS"

    view_sql = f"""
        CREATE OR REPLACE VIEW {schema_name}.{view_name} AS
        SELECT
            -- All columns from campaign data
            d.*,
            
            -- All columns from naming keys (except ad_set_name to avoid duplication)
            nk.audience,
            nk.ad_descriptor,
            nk.landing_page,
            nk.position,
            nk.concept,
            nk.ad_direction
            
        FROM {schema_name}.{campaign_data_table} d
        JOIN {schema_name}.{naming_keys_table} nk
            ON d.ad_set_name = nk.ad_set_name;
        """

    return view_sql.strip()