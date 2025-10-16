"""
Snowflake utility functions for campaign data processing.
Handles schema creation, data population, and UPSERT operations.

UNIFIED SNOWPARK APPROACH: Uses Snowpark for both local development and deployment.
No more SQLAlchemy dual code paths!
"""

import pandas as pd
import logging
from typing import Optional, Tuple
from datetime import datetime
import tempfile
import os
from snowflake.snowpark import Session
from snowpark_connection import get_snowpark_session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_snowflake_connection() -> Optional[Session]:
    """
    Get Snowpark session for both local and deployed environments.
    
    This replaces the old dual Snowpark/SQLAlchemy approach.
    Now uses Snowpark everywhere for simplicity.

    Returns:
        Snowpark Session or None if connection fails
    """
    return get_snowpark_session()


def infer_snowflake_type(series: pd.Series) -> str:
    """
    Infer Snowflake data type from pandas Series with intelligent type detection.
    Handles mixed data types and infers the most appropriate type.
    
    Args:
        series: Pandas Series to analyze
        
    Returns:
        Snowflake data type as string
    """
    # Check for already-typed columns first
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP'
    
    if pd.api.types.is_integer_dtype(series):
        return 'INTEGER'
    
    if pd.api.types.is_float_dtype(series):
        return 'FLOAT'
    
    if pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    
    # For object/mixed columns, analyze the actual data
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
            return 'INTEGER' if is_integer else 'FLOAT'
    
    # Try converting to datetime
    try:
        datetime_converted = pd.to_datetime(non_null, errors='coerce')
        datetime_success_rate = datetime_converted.notna().sum() / len(non_null)
        if datetime_success_rate > 0.8:
            return 'TIMESTAMP'
    except:
        pass
    
    # Default to STRING for everything else (including mixed types)
    return 'STRING'


def get_table_columns(table_name: str, session: Session) -> set:
    """
    Get existing column names from a Snowflake table.
    
    Args:
        table_name: Fully qualified table name (schema.table)
        session: Snowpark Session
        
    Returns:
        Set of column names (lowercase)
    """
    try:
        result = session.sql(f"DESCRIBE TABLE {table_name}").collect()
        columns = {row[0].lower() for row in result}
        logger.info(f"Table {table_name} has {len(columns)} columns")
        return columns
        
    except Exception as e:
        logger.error(f"Failed to get table columns: {e}")
        return set()


def expand_table_schema(
    table_name: str,
    df: pd.DataFrame,
    session: Session,
    exclude_columns: set = None
) -> Tuple[bool, str, int]:
    """
    Dynamically expand table schema by adding new columns found in DataFrame.
    
    Args:
        table_name: Fully qualified table name (schema.table)
        df: DataFrame with potentially new columns
        session: Snowpark Session
        exclude_columns: Set of columns to exclude from expansion (e.g., metadata columns)
        
    Returns:
        Tuple of (success: bool, message: str, columns_added: int)
    """
    try:
        if exclude_columns is None:
            exclude_columns = set()
        
        # Get existing table columns
        existing_columns = get_table_columns(table_name, session)
        
        if not existing_columns:
            return False, f"Could not read schema for table {table_name}", 0
        
        # Find new columns in DataFrame
        df_columns = {col.lower() for col in df.columns}
        new_columns = df_columns - existing_columns - {col.lower() for col in exclude_columns}
        
        if not new_columns:
            logger.info(f"No new columns to add to {table_name}")
            return True, "Schema is up to date", 0
        
        logger.info(f"Found {len(new_columns)} new columns to add: {new_columns}")
        
        # Add each new column
        columns_added = 0
        
        for col_name in new_columns:
            # Find the original case column name from df
            original_col = next((c for c in df.columns if c.lower() == col_name), col_name)
            
            # Infer data type from DataFrame
            snowflake_type = infer_snowflake_type(df[original_col])
            
            # Generate ALTER TABLE statement
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name.upper()} {snowflake_type}"
            
            try:
                logger.info(f"Adding column: {alter_sql}")
                session.sql(alter_sql).collect()
                columns_added += 1
                logger.info(f"Successfully added column {col_name.upper()} ({snowflake_type})")
                
            except Exception as col_error:
                logger.error(f"Failed to add column {col_name}: {col_error}")
                # Continue with other columns even if one fails
        
        message = f"Successfully added {columns_added} new column(s) to {table_name}"
        logger.info(message)
        return True, message, columns_added
        
    except Exception as e:
        logger.error(f"Schema expansion failed: {e}")
        return False, f"Schema expansion error: {str(e)}", 0


def create_schema_and_tables(
    client_name: str,
    platform: str,
    year: int,
    conn = None
) -> Tuple[bool, str, str]:
    """
    Create schema, stage, and platform-specific tables using sql_templates.py.

    NEW STRUCTURE: One schema per client/year with platform-prefixed tables.
    Example: CLIENT_CATERPILLAR_2024 with META_NAMING_KEYS, LINKEDIN_NAMING_KEYS, etc.

    Args:
        client_name: Client name (e.g., 'caterpillar')
        platform: Platform name (e.g., 'meta')
        year: Year (e.g., 2024)
        conn: Optional Snowflake connection (will create if not provided)

    Returns:
        Tuple of (success: bool, schema_name: str, message: str)
    """
    from sql_templates import generate_schema_creation_statements

    # New schema naming: CLIENT_{CLIENT}_{YEAR} (no platform)
    schema_name = f"CLIENT_{client_name.upper()}_{year}"

    try:
        # Get connection if not provided
        if conn is None:
            conn = get_snowflake_connection()
            if conn is None:
                return False, schema_name, "Failed to establish Snowflake connection"

        # Generate all SQL statements with platform parameter
        statements = generate_schema_creation_statements(schema_name, platform)

        # Execute statements in order
        execution_order = [
            'create_schema',
            'create_stage',
            'create_naming_keys_table',
            'create_processed_campaign_data_table',
            'create_processing_log_table'
        ]

        # Execute using Snowpark (works in both deployed and local environments)
        for statement_key in execution_order:
            if statement_key in statements:
                sql = statements[statement_key]
                logger.info(f"Executing: {statement_key} for platform {platform.upper()}")
                conn.sql(sql).collect()

        logger.info(f"Successfully created schema {schema_name} with {platform.upper()} tables")
        return True, schema_name, f"Schema {schema_name} created with {platform.upper()} tables"

    except Exception as e:
        logger.error(f"Failed to create schema and tables: {e}")
        return False, schema_name, f"Error: {str(e)}"


def rename_uploaded_file(
    original_filename: str,
    wave_number: int,
    client_name: str,
    platform: str,
    year: int,
    file_type: str
) -> str:
    """
    Generate standardized filename for uploaded files.

    Format: {client}_{platform}_{year}_wave{N}_{description}.csv
    Example: caterpillar_meta_2024_wave1_campaigns.csv

    Args:
        original_filename: Original uploaded filename
        wave_number: Wave number
        client_name: Client name
        platform: Platform name
        year: Year
        file_type: 'campaigns' or 'naming_keys'

    Returns:
        Standardized filename
    """
    client_clean = client_name.lower().replace(' ', '_')
    platform_clean = platform.lower().replace(' ', '_')

    standardized_name = f"{client_clean}_{platform_clean}_{year}_wave{wave_number}_{file_type}.csv"

    logger.info(f"Renamed '{original_filename}' to '{standardized_name}'")
    return standardized_name


def save_renamed_file(uploaded_file, renamed_filename: str, output_dir: Optional[str] = None) -> str:
    """
    Save uploaded file with new standardized name.

    Args:
        uploaded_file: Streamlit UploadedFile object or file path
        renamed_filename: New filename to use
        output_dir: Optional directory to save to (uses temp dir if not provided)

    Returns:
        Path to saved file
    """
    if output_dir is None:
        output_dir = tempfile.gettempdir()

    output_path = os.path.join(output_dir, renamed_filename)

    # Handle Streamlit UploadedFile
    if hasattr(uploaded_file, 'getvalue'):
        with open(output_path, 'wb') as f:
            f.write(uploaded_file.getvalue())
    # Handle file path string
    elif isinstance(uploaded_file, str):
        import shutil
        shutil.copy2(uploaded_file, output_path)
    else:
        raise ValueError(f"Unsupported file type: {type(uploaded_file)}")

    logger.info(f"Saved file to: {output_path}")
    return output_path


def populate_naming_keys_table(
    df: pd.DataFrame,
    schema_name: str,
    platform: str,
    wave_number: int,
    conn = None
) -> Tuple[bool, str]:
    """
    Populate platform-prefixed naming_keys table with UPSERT logic.
    ON CONFLICT (ad_set_name) DO UPDATE.

    NEW STRUCTURE: Uses platform-prefixed table names.
    Example: CLIENT_CATERPILLAR_2024.META_NAMING_KEYS

    Args:
        df: Processed naming keys DataFrame
        schema_name: Target schema name (e.g., CLIENT_CATERPILLAR_2024)
        platform: Platform name (e.g., 'meta')
        wave_number: Wave number
        conn: Snowflake connection

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Get connection if not provided
        if conn is None:
            conn = get_snowflake_connection()
            if conn is None:
                return False, "Failed to establish Snowflake connection"

        warning_msg = None

        # Ensure wave_number column exists
        if 'wave_number' not in df.columns:
            df['wave_number'] = wave_number

        # Add upload timestamp
        df['upload_timestamp'] = datetime.now()

        # Platform-prefixed table name
        prefix = platform.upper()
        naming_keys_table = f"{prefix}_NAMING_KEYS"
        full_table_name_for_schema = f"{schema_name}.{naming_keys_table}"
        
        # SCHEMA EXPANSION: Check and add new columns if found
        logger.info(f"Checking schema for new columns in {full_table_name_for_schema}")
        expand_success, expand_msg, cols_added = expand_table_schema(
            full_table_name_for_schema,
            df,
            conn,
            exclude_columns={'upload_timestamp'}  
        )
        
        if cols_added > 0:
            logger.info(f"Schema expanded: {expand_msg}")
        elif not expand_success:
            logger.warning(f"Schema expansion check failed: {expand_msg}")

        # Refresh table columns after schema expansion
        existing_columns = get_table_columns(full_table_name_for_schema, conn)

        # Use only columns present in both DataFrame and table
        filtered_columns = [col for col in df.columns if col.lower() in existing_columns]
        missing_for_upload = [col for col in df.columns if col.lower() not in existing_columns]

        warning_msg = None
        if missing_for_upload:
            warning_msg = (
                "⚠️ Naming data is missing %d column(s) present in Snowflake table: %s. "
                "Existing rows keep old values, new rows will have NULLs for these columns."
            ) % (len(missing_for_upload), missing_for_upload)
            logger.warning(warning_msg)

        if not filtered_columns:
            return False, "No matching columns between uploaded naming data and table schema"

        df_filtered = df[filtered_columns].copy()

        # Snowpark: write DataFrame to temp table within target schema
        temp_table_unqualified = f"{prefix}_NAMING_KEYS_TEMP_{wave_number}"

        # Determine current database and fully qualify all table references
        _db_row = conn.sql("SELECT CURRENT_DATABASE() AS DB").collect()[0]
        current_db = _db_row[0] if isinstance(_db_row, (list, tuple)) else _db_row["DB"]

        full_temp_table = f"{current_db}.{schema_name}.{temp_table_unqualified}"
        full_table_name = f"{current_db}.{schema_name}.{naming_keys_table}"

        logger.info(f"Creating temp table: {full_temp_table}")

        # Create Snowpark DataFrame and save into the target schema explicitly
        sp_df = conn.create_dataframe(df_filtered)
        sp_df.write.save_as_table(full_temp_table, mode="overwrite")

        logger.info("Temp table created successfully")

        # Generate MERGE statement dynamically based on actual columns
        # Note: Snowpark DataFrames preserve column case, so we use exact case from df_filtered
        all_columns = df_filtered.columns.tolist()

        # Build UPDATE SET clause (exclude primary key)
        update_columns = [col for col in all_columns if col.lower() != 'ad_set_name']
        update_set_clause = ', '.join([f'{col} = source."{col}"' for col in update_columns])

        # Build INSERT clause
        insert_columns = ', '.join(all_columns)
        insert_values = ', '.join([f'source."{col}"' for col in all_columns])

        merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING {full_temp_table} AS source
            ON target.ad_set_name = source."ad_set_name"
            WHEN MATCHED THEN UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
                VALUES ({insert_values})
        """

        logger.info("Executing MERGE statement")
        # logger.info(f"MERGE SQL: {merge_sql[:500]}...")

        # Execute MERGE with error handling and guaranteed cleanup
        try:
            conn.sql(merge_sql).collect()
            logger.info("MERGE completed successfully")
        except Exception as merge_error:
            logger.error(f"MERGE failed: {merge_error}")
            logger.error(f"Full MERGE SQL: {merge_sql}")
            raise
        finally:
            # Always drop temp table, even if MERGE fails
            try:
                conn.sql(f"DROP TABLE IF EXISTS {full_temp_table}").collect()
                logger.info(f"Dropped temp table: {full_temp_table}")
            except Exception as drop_error:
                logger.warning(f"Failed to drop temp table {full_temp_table}: {drop_error}")

        result_message = f"Inserted/Updated {len(df_filtered)} naming key records"
        if warning_msg:
            result_message = f"{result_message} | {warning_msg}"

        logger.info(f"Successfully populated naming_keys table with {len(df_filtered)} records")
        return True, result_message

    except Exception as e:
        logger.error(f"Failed to populate naming_keys table: {e}")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception details: {repr(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False, f"Error: {str(e)}"


def populate_campaign_data_table(
    df: pd.DataFrame,
    schema_name: str,
    platform: str,
    wave_number: int,
    conn = None
) -> Tuple[bool, str]:
    """
    Populate platform-prefixed processed_campaign_data table with UPSERT logic.
    ON CONFLICT (ad_name) DO UPDATE.

    NEW STRUCTURE: Uses platform-prefixed table names.
    Example: CLIENT_CATERPILLAR_2024.META_PROCESSED_CAMPAIGN_DATA

    Args:
        df: Processed campaign data DataFrame
        schema_name: Target schema name (e.g., CLIENT_CATERPILLAR_2024)
        platform: Platform name (e.g., 'meta')
        wave_number: Wave number
        conn: Snowflake connection

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Get connection if not provided
        if conn is None:
            conn = get_snowflake_connection()
            if conn is None:
                return False, "Failed to establish Snowflake connection"

        # Ensure wave_number column exists
        if 'wave_number' not in df.columns:
            df['wave_number'] = wave_number

        # Add upload timestamp
        df['upload_timestamp'] = datetime.now()

        # Platform-prefixed table name
        prefix = platform.upper()
        campaign_data_table = f"{prefix}_PROCESSED_CAMPAIGN_DATA"
        full_table_name_for_schema = f"{schema_name}.{campaign_data_table}"
        
        # SCHEMA EXPANSION: Check and add new columns if found
        logger.info(f"Checking schema for new columns in {full_table_name_for_schema}")
        expand_success, expand_msg, cols_added = expand_table_schema(
            full_table_name_for_schema,
            df,
            conn,
            exclude_columns={'upload_timestamp'}  # Exclude metadata columns from expansion check
        )
        
        if cols_added > 0:
            logger.info(f"Schema expanded: {expand_msg}")
        elif not expand_success:
            logger.warning(f"Schema expansion check failed: {expand_msg}")

        # Refresh table columns after schema expansion
        existing_columns = get_table_columns(full_table_name_for_schema, conn)

        # Use only columns present in both DataFrame and table
        filtered_columns = [col for col in df.columns if col.lower() in existing_columns]
        missing_for_upload = [col for col in df.columns if col.lower() not in existing_columns]

        warning_msg = None
        if missing_for_upload:
            warning_msg = (
                "⚠️ Campaign data is missing %d column(s) present in Snowflake table: %s. "
                "Existing rows keep old metrics, new rows will have NULLs."
            ) % (len(missing_for_upload), missing_for_upload)
            logger.warning(warning_msg)

        if not filtered_columns:
            return False, "No matching columns between uploaded campaign data and table schema"

        df_filtered = df[filtered_columns].copy()

        # Snowpark session: create temp table within target schema
        temp_table_unqualified = f"{prefix}_CAMPAIGN_DATA_TEMP_{wave_number}"

        # Determine current database and fully qualify all table references
        _db_row = conn.sql("SELECT CURRENT_DATABASE() AS DB").collect()[0]
        current_db = _db_row[0] if isinstance(_db_row, (list, tuple)) else _db_row["DB"]

        full_temp_table = f"{current_db}.{schema_name}.{temp_table_unqualified}"
        full_table_name = f"{current_db}.{schema_name}.{campaign_data_table}"

        logger.info(f"Creating campaign temp table: {full_temp_table}")

        # Create Snowpark DataFrame and save into the target schema explicitly
        sp_df = conn.create_dataframe(df_filtered)
        sp_df.write.save_as_table(full_temp_table, mode="overwrite")
        
        logger.info("Campaign temp table created successfully")

        # Build UPDATE SET clause dynamically (exclude primary key) with quoted column names
        all_columns = df_filtered.columns.tolist()
        update_columns = [col for col in all_columns if col.lower() != 'ad_name']
        update_set_clause = ', '.join([f'{col} = source."{col}"' for col in update_columns])

        # Build INSERT clause dynamically with quoted column names
        insert_columns = ', '.join(all_columns)
        insert_values = ', '.join([f'source."{col}"' for col in all_columns])

        # Generate MERGE statement with fully qualified names and quoted column references
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING {full_temp_table} AS source
        ON target.ad_name = source."ad_name"
        WHEN MATCHED THEN UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        logger.info("Executing campaign MERGE statement")
        
        # Execute MERGE with error handling and guaranteed cleanup
        try:
            conn.sql(merge_sql).collect()
            logger.info("Campaign MERGE completed successfully")
        except Exception as merge_error:
            logger.error(f"Campaign MERGE failed: {merge_error}")
            logger.error(f"Full MERGE SQL: {merge_sql}")
            raise
        finally:
            # Always drop temp table, even if MERGE fails
            try:
                conn.sql(f"DROP TABLE IF EXISTS {full_temp_table}").collect()
                logger.info(f"Dropped temp table: {full_temp_table}")
            except Exception as drop_error:
                logger.warning(f"Failed to drop temp table {full_temp_table}: {drop_error}")

        result_message = f"Inserted/Updated {len(df_filtered)} campaign data records"
        if warning_msg:
            result_message = f"{result_message} | {warning_msg}"

        logger.info(f"Successfully populated campaign_data table with {len(df_filtered)} records")
        return True, result_message

    except Exception as e:
        logger.error(f"Failed to populate campaign_data table: {e}")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception details: {repr(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False, f"Error: {str(e)}"


def upload_csv_to_stage(
    uploaded_file,
    schema_name: str,
    wave_number: int,
    file_type: str,
    client_name: str,
    platform: str,
    year: int,
    conn = None
) -> Tuple[bool, str]:
    """
    Upload CSV file to Snowflake stage.

    Args:
        uploaded_file: Streamlit UploadedFile object
        schema_name: Schema name
        wave_number: Wave number
        file_type: 'campaigns' or 'naming_keys'
        client_name: Client name
        platform: Platform name
        year: Year
        conn: Snowflake connection

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Get connection if not provided
        if conn is None:
            conn = get_snowflake_connection()
            if conn is None:
                return False, "Failed to establish Snowflake connection"

        # Generate standardized filename
        standardized_filename = rename_uploaded_file(
            uploaded_file.name, wave_number, client_name, platform, year, file_type
        )

        # Save file to temp location
        temp_path = save_renamed_file(uploaded_file, standardized_filename)

        # Stage name
        stage_name = f"{schema_name}.{schema_name}_stage"

        # Use Snowpark file API when available; PUT is not supported in Streamlit runtime
        has_file_api = hasattr(conn, 'file') and hasattr(getattr(conn, 'file'), 'put')

        if has_file_api:
            # Snowpark session in Snowflake (Streamlit): use session.file.put
            stage_location = f"@{stage_name}"
            conn.file.put(
                local_file_name=temp_path,
                stage_location=stage_location,
                overwrite=True,
                auto_compress=False
            )
        else:
            # Fallback for Snowpark-like sessions: execute PUT via SQL (works in local dev)
            put_sql = f"PUT file://{temp_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            conn.sql(put_sql).collect()

        # Clean up temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

        logger.info(f"Successfully uploaded {standardized_filename} to stage {stage_name}")
        return True, f"Uploaded {standardized_filename} to stage"

    except Exception as e:
        logger.error(f"Failed to upload file to stage: {e}")
        return False, f"Error uploading to stage: {str(e)}"


def create_audience_ad_descriptor_view(
    schema_name: str,
    platform: str,
    conn = None
) -> Tuple[bool, str]:
    """
    Create platform-prefixed AUDIENCE_AD_DESCRIPTOR_DATA view in the schema.
    
    This view joins ALL columns from both NAMING_KEYS and PROCESSED_CAMPAIGN_DATA tables.
    Single comprehensive view suitable for all use cases.

    NEW STRUCTURE: Creates platform-specific views.
    Example: CLIENT_CATERPILLAR_2024.META_AUDIENCE_AD_DESCRIPTOR_DATA

    Args:
        schema_name: Schema name (e.g., CLIENT_CATERPILLAR_2024)
        platform: Platform name (e.g., 'meta')
        conn: Snowflake connection

    Returns:
        Tuple of (success: bool, message: str)
    """
    from sql_templates import generate_view_creation_statement

    try:
        # Get connection if not provided
        if conn is None:
            conn = get_snowflake_connection()
            if conn is None:
                return False, "Failed to establish Snowflake connection"

        # Platform-prefixed view name
        view_name = f"{platform.upper()}_AUDIENCE_AD_DESCRIPTOR_DATA"
        full_view_name = f"{schema_name}.{view_name}"

        # Verify that source tables exist before creating view
        prefix = platform.upper()
        campaign_table = f"{schema_name}.{prefix}_PROCESSED_CAMPAIGN_DATA"
        naming_table = f"{schema_name}.{prefix}_NAMING_KEYS"

        logger.info("Verifying source tables exist for view creation")

        try:
            # Verify tables exist
            conn.sql(f"SELECT 1 FROM {campaign_table} LIMIT 1").collect()
            conn.sql(f"SELECT 1 FROM {naming_table} LIMIT 1").collect()
        except Exception as e:
            error_msg = f"Source tables do not exist or are not accessible: {e}"
            logger.error(error_msg)
            return False, error_msg

        # Generate view creation SQL
        view_sql = generate_view_creation_statement(schema_name, platform)
        logger.info(f"Creating view: {full_view_name}")

        conn.sql(view_sql).collect()
        logger.info(f"Successfully created {view_name} view in {schema_name}")

        return True, f"Created view {full_view_name}"

    except Exception as e:
        logger.error(f"Failed to create view: {e}")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception details: {repr(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False, f"Error creating view: {str(e)}"


def insert_processing_log(
    schema_name: str,
    wave_number: int,
    status: str,
    records_processed: int,
    errors_count: int,
    warnings_count: int,
    processing_time: float,
    client_name: str,
    platform: str,
    year: int,
    conn = None
) -> Tuple[bool, str]:
    """
    Insert a record into the single shared PROCESSING_LOG table.

    NEW STRUCTURE: Single PROCESSING_LOG table per schema (not platform-prefixed).
    The platform column distinguishes between different platforms.
    Example: CLIENT_CATERPILLAR_2024.PROCESSING_LOG

    Args:
        schema_name: Schema name (e.g., CLIENT_CATERPILLAR_2024)
        wave_number: Wave number
        status: Processing status ('SUCCESS' or 'FAILED')
        records_processed: Number of records processed
        errors_count: Number of errors
        warnings_count: Number of warnings
        processing_time: Processing time in seconds
        client_name: Client name
        platform: Platform name (e.g., 'meta')
        year: Year
        conn: Snowflake connection

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Get connection if not provided
        if conn is None:
            conn = get_snowflake_connection()
            if conn is None:
                return False, "Failed to establish Snowflake connection"

        # Single shared table name (not platform-prefixed)
        processing_log_table = "PROCESSING_LOG"
        full_table_name = f"{schema_name}.{processing_log_table}"

        # Build INSERT statement
        insert_sql = f"""
            INSERT INTO {full_table_name} (
                wave_number,
                processing_timestamp,
                status,
                records_processed,
                errors_count,
                warnings_count,
                processing_time_seconds,
                client_name,
                platform,
                year
            ) VALUES (
                {wave_number},
                CURRENT_TIMESTAMP(),
                '{status}',
                {records_processed},
                {errors_count},
                {warnings_count},
                {processing_time},
                '{client_name}',
                '{platform}',
                {year}
            )
            """

        logger.info(f"Inserting processing log for wave {wave_number}, platform {platform}")

        conn.sql(insert_sql).collect()

        logger.info(f"Successfully inserted processing log for wave {wave_number}, platform {platform}")
        return True, "Processing log inserted successfully"

    except Exception as e:
        logger.error(f"Failed to insert processing log: {e}")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception details: {repr(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False, f"Error: {str(e)}"