"""
Spark No. 9 - Campaign Data Processor
Simple Streamlit application for processing campaign data.
"""

import streamlit as st
import pandas as pd
import tempfile
import os
import time
from datetime import datetime
from data_processor import CampaignDataProcessor
from snowflake_operations import (
    get_snowflake_connection,
    create_schema_and_tables,
    rename_uploaded_file,
    populate_naming_keys_table,
    populate_campaign_data_table,
    insert_processing_log,
    upload_csv_to_stage,
    create_audience_ad_descriptor_view
)
import constants

# Page configuration
st.set_page_config(
    page_title=constants.APP_TITLE,
    page_icon=constants.APP_ICON,
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'About': "Campaign Data Processing System v1.0"
    }
)

def upload_to_snowflake(conn, campaign_df, naming_df, schema_name, wave_number, client_name, platform, year, start_time, campaign_file, naming_file):
    """
    Upload processed data to Snowflake with UPSERT logic.

    Args:
        conn: Snowflake connection
        campaign_df: Processed campaign DataFrame
        naming_df: Processed naming DataFrame
        schema_name: Schema name
        wave_number: Wave number
        client_name: Client name
        platform: Platform name
        year: Year
        start_time: Processing start time for calculating duration
        campaign_file: Campaign uploaded file object
        naming_file: Naming uploaded file object

    Returns:
        Tuple of (success: bool, messages: list)
    """
    messages = []
    errors_count = 0
    warnings_count = 0

    try:
        # Step 1: Create schema and tables
        st.write("Creating schema and tables...")
        schema_success, schema_name_result, schema_msg = create_schema_and_tables(
            client_name, platform, year, conn
        )
        messages.append(schema_msg)

        if not schema_success:
            errors_count += 1
            return False, messages

        st.write(f"✅ Schema created: {schema_name_result}")

        # Step 2: Upload CSV files to stage
        st.write("Uploading CSV files to Snowflake stage...")

        # Upload campaign file (for backup/audit - non-critical)
        campaign_stage_success, campaign_stage_msg = upload_csv_to_stage(
            campaign_file, schema_name, wave_number, 'campaigns',
            client_name, platform, year, conn
        )
        messages.append(campaign_stage_msg)

        if campaign_stage_success:
            st.write(f"✅ {campaign_stage_msg}")
        else:
            warnings_count += 1
            st.write(f"⚠️ {campaign_stage_msg} (non-critical, continuing...)")

        # Upload naming file (for backup/audit - non-critical)
        naming_stage_success, naming_stage_msg = upload_csv_to_stage(
            naming_file, schema_name, wave_number, 'naming_keys',
            client_name, platform, year, conn
        )
        messages.append(naming_stage_msg)

        if naming_stage_success:
            st.write(f"✅ {naming_stage_msg}")
        else:
            warnings_count += 1
            st.write(f"⚠️ {naming_stage_msg} (non-critical, continuing...)")

        # Step 3: Populate naming keys table
        st.write("Populating naming keys table...")
        naming_success, naming_msg = populate_naming_keys_table(
            naming_df, schema_name, platform, wave_number, conn
        )
        messages.append(naming_msg)

        if not naming_success:
            errors_count += 1
            return False, messages

        st.write(f"✅ {naming_msg}")

        # Step 4: Populate campaign data table
        st.write("Populating campaign data table...")
        campaign_success, campaign_msg = populate_campaign_data_table(
            campaign_df, schema_name, platform, wave_number, conn
        )
        messages.append(campaign_msg)

        if not campaign_success:
            errors_count += 1
            return False, messages

        st.write(f"✅ {campaign_msg}")

        # Step 5: Create AUDIENCE_AD_DESCRIPTOR_DATA view
        st.write("Creating AUDIENCE_AD_DESCRIPTOR_DATA view...")
        view_success, view_msg = create_audience_ad_descriptor_view(schema_name, platform, conn)
        messages.append(view_msg)

        if view_success:
            st.write(f"✅ {view_msg}")
        else:
            errors_count += 1
            st.write(f"⚠️ {view_msg}")

        # Step 6: Insert processing log
        processing_time = time.time() - start_time
        total_records = len(campaign_df) + len(naming_df)

        log_success, log_msg = insert_processing_log(
            schema_name, wave_number, 'SUCCESS', total_records,
            errors_count, warnings_count, processing_time,
            client_name, platform, year, conn
        )
        messages.append(log_msg)

        if log_success:
            st.write(f"✅ Processing log inserted")

        return True, messages

    except Exception as e:
        error_msg = f"Snowflake upload failed: {str(e)}"
        messages.append(error_msg)
        st.error(error_msg)
        return False, messages

def process_uploaded_files(campaign_file, naming_file, wave_number, client_name, platform, project_year):
    """Process uploaded files and return result."""
    try:
        # Save uploaded files temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_campaign:
            tmp_campaign.write(campaign_file.getvalue())
            campaign_path = tmp_campaign.name
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_naming:
            tmp_naming.write(naming_file.getvalue())
            naming_path = tmp_naming.name
        
        # Initialize processor
        processor = CampaignDataProcessor()
        
        # Process files
        result = processor.process_files(campaign_path, naming_path, wave_number, client_name, platform, project_year)
        
        # Clean up temporary files
        os.unlink(campaign_path)
        os.unlink(naming_path)
        
        return result
        
    except Exception as e:
        return {'success': False, 'errors': [str(e)]}


def main():
    """Main application function"""
    st.title(f"{constants.APP_ICON} {constants.APP_TITLE}")
    st.markdown("Upload campaign data files and process them automatically")

    # File Upload Section
    st.header("📁 Upload Files")

    col1, col2 = st.columns(2)

    with col1:
        campaign_file = st.file_uploader(
            "Campaign Data File",
            type=constants.ALLOWED_FILE_TYPES,
            help="Upload campaign data CSV file"
        )

    with col2:
        naming_file = st.file_uploader(
            "Naming Key File",
            type=constants.ALLOWED_FILE_TYPES,
            help="Upload naming key CSV file"
        )

    # Metadata Form
    st.header("📝 File Metadata")

    col1, col2, col3 = st.columns(3)

    with col1:
        client_name = st.text_input("Client Code", value=constants.DEFAULT_CLIENT)
        platform = st.selectbox("Platform", constants.VALID_PLATFORMS, index=0)

    with col2:
        wave_number = st.number_input(
            "Wave Number",
            min_value=constants.MIN_WAVE_NUMBER,
            max_value=constants.MAX_WAVE_NUMBER
        )
        project_year = st.number_input(
            "Project Year",
            min_value=constants.MIN_PROJECT_YEAR,
            max_value=constants.MAX_PROJECT_YEAR,
            value=datetime.now().year
        )
    
    
    # Process Button
    st.markdown("---")
    
    if st.button("🚀 Process Files", type="primary", use_container_width=True):
        if not campaign_file or not naming_file:
            st.error("❌ Please upload both campaign data and naming key files")
        else:
            # Start processing timer
            start_time = time.time()

            # Processing steps with logging
            st.header("📊 Processing Steps")

            # Step 1: File renaming
            with st.status("Step 1: Renaming files...", expanded=True) as status:
                st.write("Standardizing file names...")

                # Generate standardized filenames
                campaign_renamed = rename_uploaded_file(
                    campaign_file.name, wave_number, client_name, platform, project_year, 'campaigns'
                )
                naming_renamed = rename_uploaded_file(
                    naming_file.name, wave_number, client_name, platform, project_year, 'naming_keys'
                )

                st.write(f"✅ Campaign file: `{campaign_renamed}`")
                st.write(f"✅ Naming file: `{naming_renamed}`")

                status.update(label="Step 1: File renaming completed", state="complete")

            # Step 2: File validation and preview
            with st.status("Step 2: Validating files...", expanded=True) as status:
                st.write("✅ Campaign file uploaded")
                st.write("✅ Naming key file uploaded")

                # Preview data
                st.write("📊 **Data Preview:**")

                # Campaign data preview
                try:
                    campaign_df = pd.read_csv(campaign_file)
                    st.write(f"**Campaign Data:** {len(campaign_df)} rows, {len(campaign_df.columns)} columns")
                    st.dataframe(campaign_df.head(3), use_container_width=True)
                except Exception as e:
                    st.write(f"❌ Error reading campaign file: {str(e)}")

                # Naming data preview
                try:
                    naming_df = pd.read_csv(naming_file)
                    st.write(f"**Naming Data:** {len(naming_df)} rows, {len(naming_df.columns)} columns")
                    st.dataframe(naming_df.head(3), use_container_width=True)
                except Exception as e:
                    st.write(f"❌ Error reading naming file: {str(e)}")

                status.update(label="Step 2: File validation completed", state="complete")

            # Step 3: Data processing
            with st.status("Step 3: Processing data...", expanded=True) as status:
                try:
                    result = process_uploaded_files(campaign_file, naming_file, wave_number, client_name, platform, project_year)

                    if result['success']:
                        st.write("✅ Data validation completed")
                        st.write(f"✅ Processed {len(result.get('campaign_data', []))} campaign records")
                        st.write(f"✅ Processed {len(result.get('naming_data', []))} naming records")
                        status.update(label="Step 3: Data processing completed", state="complete")
                    else:
                        st.write("❌ Data processing failed")
                        for error in result.get('errors', []):
                            st.write(f"❌ {error}")
                        status.update(label="Step 3: Data processing failed", state="error")

                except Exception as e:
                    st.write(f"❌ Unexpected error: {str(e)}")
                    status.update(label="Step 3: Processing failed", state="error")

            # Step 4: Upload to Snowflake (only if Step 3 succeeded)
            if result.get('success', False):
                # Track overall success (starts as True, becomes False if upload fails)
                overall_success = True

                with st.status("Step 4: Uploading to Snowflake...", expanded=True) as upload_status:
                    # Get Snowflake connection
                    conn = get_snowflake_connection()

                    if conn:
                        st.write("✅ Snowflake connection established")

                        upload_success, upload_messages = upload_to_snowflake(
                            conn,
                            result['campaign_data'],
                            result['naming_data'],
                            result['schema_name'],
                            wave_number, client_name, platform, project_year,
                            start_time,
                            campaign_file,
                            naming_file
                        )

                        if upload_success:
                            st.write("✅ Data uploaded to Snowflake successfully")
                            upload_status.update(label="Step 4: Snowflake upload completed", state="complete")
                        else:
                            st.write("❌ Snowflake upload failed")
                            for msg in upload_messages:
                                st.write(f"❌ {msg}")
                            upload_status.update(label="Step 4: Snowflake upload failed", state="error")
                            overall_success = False
                    else:
                        st.write("⚠️ No Snowflake connection available - running in local mode")
                        st.write("💡 Files have been processed and validated locally")
                        upload_status.update(label="Step 4: Skipped (local mode)", state="complete")

                # Step 5: Finalize only if overall success
                if overall_success:
                    with st.status("Step 5: Finalizing...", expanded=True) as final_status:
                        processing_time = time.time() - start_time
                        st.write("✅ All processing steps completed successfully")
                        st.write(f"⏱️ Total processing time: {processing_time:.2f} seconds")
                        final_status.update(label="Step 5: Processing completed successfully", state="complete")

                    # Success message - only show if everything succeeded
                    st.success("🎉 **Processing Successful!**")

                    # Show results
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Wave", wave_number)
                    with col2:
                        st.metric("Client", client_name.upper())
                    with col3:
                        st.metric("Platform", platform.upper())
                    with col4:
                        st.metric("Year", project_year)

                    if 'campaign_data' in result and 'naming_data' in result:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Campaign Records", len(result['campaign_data']))
                        with col2:
                            st.metric("Naming Records", len(result['naming_data']))

                    # Show warnings if any
                    if result.get('warnings'):
                        st.warning("⚠️ **Warnings:**")
                        for warning in result['warnings']:
                            st.write(f"- {warning}")
                else:
                    # Show error message if upload failed
                    st.error("❌ **Upload to Snowflake Failed!**")
                    st.info("Data was validated and processed locally, but could not be uploaded to Snowflake.")

            # Show error details outside the status context to avoid nesting issues
            if not result.get('success', False):
                st.error("❌ **Processing Failed!**")
                st.markdown("**Error Details:**")
                for error in result.get('errors', []):
                    st.error(f"• {error}")
    
    # Footer
    st.markdown("---")
    st.markdown("**Spark No. 9** - Campaign Data Processing System")


if __name__ == "__main__":
    main()