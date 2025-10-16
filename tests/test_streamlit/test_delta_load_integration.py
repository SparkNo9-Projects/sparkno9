"""
Integration tests for delta load functionality with real Snowflake database.
Tests that latest data overrides old values of the same row.

Prerequisites:
- Set up .env file with Snowflake credentials or configure Streamlit secrets
- Requires SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
"""

import pytest
import pandas as pd
import sys
import logging
from pathlib import Path

# Configure logging for tests
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Add src to path for imports
src_path = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(src_path))

# Also add streamlit directory so modules can find each other
streamlit_path = src_path / 'project_spark' / 'streamlit'
sys.path.insert(0, str(streamlit_path))

from project_spark.streamlit.snowflake_operations import (
    populate_campaign_data_table,
    populate_naming_keys_table,
    create_schema_and_tables,
    get_snowflake_connection
)


@pytest.fixture(scope="module")
def snowflake_session():
    """Get real Snowflake session for integration tests"""
    logger.info("Attempting to connect to Snowflake...")
    session = get_snowflake_connection()
    if session is None:
        logger.error("Failed to connect to Snowflake!")
        logger.error("Check your .env file for: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE")
        pytest.skip("Snowflake connection not available. Set up .env or Streamlit secrets.")
    logger.info("Successfully connected to Snowflake!")
    yield session
    session.close()


@pytest.fixture(scope="module")
def test_database(snowflake_session):
    """Create test database and cleanup after all tests"""
    test_db_name = "TEST_DELTA_LOAD_DB"

    try:
        # Create test database
        logger.info(f"Creating test database: {test_db_name}")
        snowflake_session.sql(f"CREATE DATABASE IF NOT EXISTS {test_db_name}").collect()

        # Switch to test database
        snowflake_session.sql(f"USE DATABASE {test_db_name}").collect()
        logger.info(f"Using test database: {test_db_name}")

        yield test_db_name

    finally:
        # Cleanup: Drop test database after all tests
        try:
            logger.info(f"Cleaning up test database: {test_db_name}")
            snowflake_session.sql(f"DROP DATABASE IF EXISTS {test_db_name} CASCADE").collect()
            logger.info(f"Test database {test_db_name} dropped successfully")
        except Exception as e:
            logger.warning(f"Failed to cleanup test database: {e}")


@pytest.fixture(scope="module")
def test_schema(snowflake_session, test_database):
    """Create test schema and tables, cleanup after tests"""
    test_client = "test_delta"
    test_platform = "meta"
    test_year = 2024

    # Create schema and tables
    success, schema_name, message = create_schema_and_tables(
        test_client, test_platform, test_year, snowflake_session
    )

    if not success:
        pytest.skip(f"Failed to create test schema: {message}")

    yield {
        'schema_name': schema_name,
        'platform': test_platform,
        'client': test_client,
        'year': test_year,
        'database': test_database
    }

    # Note: Schema will be cleaned up when database is dropped


class TestDeltaLoadCampaignDataIntegration:
    """Integration tests for campaign data delta load with real database"""

    def test_campaign_data_latest_overrides_old(self, snowflake_session, test_schema):
        """
        Test that when same ad_name is inserted twice, latest data overrides old values.
        This is the core delta load test.
        """
        schema_name = test_schema['schema_name']
        platform = test_schema['platform']

        # STEP 1: Insert initial data (Wave 1)
        initial_data = pd.DataFrame({
            'ad_name': ['DeltaTest_Ad1', 'DeltaTest_Ad2'],
            'campaign_name': ['Campaign_Initial', 'Campaign_Initial2'],
            'amount_spent_usd': [100.0, 200.0],
            'impressions': [1000, 2000],
            'reach': [500, 1000],
            'wave_number': [1, 1]
        })

        success1, message1 = populate_campaign_data_table(
            initial_data, schema_name, platform, 1, snowflake_session
        )

        assert success1 is True, f"Initial insert failed: {message1}"

        # STEP 2: Verify initial data is in database
        table_name = f"{schema_name}.{platform.upper()}_PROCESSED_CAMPAIGN_DATA"
        query1 = f"""
            SELECT ad_name, campaign_name, amount_spent_usd, impressions, reach, wave_number
            FROM {table_name}
            WHERE ad_name = 'DeltaTest_Ad1'
            ORDER BY wave_number DESC
        """
        result1 = snowflake_session.sql(query1).collect()

        logger.info("=" * 80)
        logger.info("BEFORE DELTA LOAD - Initial Data (Wave 1)")
        logger.info("=" * 80)
        assert len(result1) == 1, "Should have 1 record for DeltaTest_Ad1"
        row1 = result1[0]
        logger.info(f"ad_name: {row1['AD_NAME']}")
        logger.info(f"campaign_name: {row1['CAMPAIGN_NAME']}")
        logger.info(f"amount_spent_usd: {float(row1['AMOUNT_SPENT_USD'])}")
        logger.info(f"impressions: {int(row1['IMPRESSIONS'])}")
        logger.info(f"reach: {int(row1['REACH'])}")
        logger.info(f"wave_number: {int(row1['WAVE_NUMBER'])}")
        logger.info("=" * 80)

        assert row1['CAMPAIGN_NAME'] == 'Campaign_Initial'
        assert float(row1['AMOUNT_SPENT_USD']) == 100.0
        assert int(row1['IMPRESSIONS']) == 1000
        assert int(row1['REACH']) == 500
        assert int(row1['WAVE_NUMBER']) == 1

        # STEP 3: Insert updated data for same ad_name (Wave 2) - Delta Load
        updated_data = pd.DataFrame({
            'ad_name': ['DeltaTest_Ad1'],  # Same ad_name as before
            'campaign_name': ['Campaign_Updated'],  # NEW VALUE
            'amount_spent_usd': [250.0],  # NEW VALUE (was 100.0)
            'impressions': [2500],  # NEW VALUE (was 1000)
            'reach': [1200],  # NEW VALUE (was 500)
            'wave_number': [2]  # NEW WAVE
        })

        success2, message2 = populate_campaign_data_table(
            updated_data, schema_name, platform, 2, snowflake_session
        )

        assert success2 is True, f"Delta load (update) failed: {message2}"

        # STEP 4: Verify that old values were REPLACED by new values (not added as new row)
        query2 = f"""
            SELECT ad_name, campaign_name, amount_spent_usd, impressions, reach, wave_number
            FROM {table_name}
            WHERE ad_name = 'DeltaTest_Ad1'
            ORDER BY wave_number DESC
        """
        result2 = snowflake_session.sql(query2).collect()

        logger.info("")
        logger.info("=" * 80)
        logger.info("AFTER DELTA LOAD - Updated Data (Wave 2)")
        logger.info("=" * 80)
        # Critical assertion: Should still be only 1 row (updated, not inserted as new)
        assert len(result2) == 1, "Delta load should UPDATE existing row, not INSERT new row"
        logger.info(f"Number of rows: {len(result2)} (should be 1 - proving UPDATE not INSERT)")

        row2 = result2[0]
        logger.info(f"ad_name: {row2['AD_NAME']}")
        logger.info(f"campaign_name: {row2['CAMPAIGN_NAME']} (was 'Campaign_Initial')")
        logger.info(f"amount_spent_usd: {float(row2['AMOUNT_SPENT_USD'])} (was 100.0)")
        logger.info(f"impressions: {int(row2['IMPRESSIONS'])} (was 1000)")
        logger.info(f"reach: {int(row2['REACH'])} (was 500)")
        logger.info(f"wave_number: {int(row2['WAVE_NUMBER'])} (was 1)")
        logger.info("=" * 80)

        # Verify new values have OVERRIDDEN old values
        assert row2['CAMPAIGN_NAME'] == 'Campaign_Updated', "Campaign name should be updated"
        assert float(row2['AMOUNT_SPENT_USD']) == 250.0, "Amount spent should be updated to 250.0"
        assert int(row2['IMPRESSIONS']) == 2500, "Impressions should be updated to 2500"
        assert int(row2['REACH']) == 1200, "Reach should be updated to 1200"
        assert int(row2['WAVE_NUMBER']) == 2, "Wave number should be updated to 2"

        logger.info("TEST PASSED: Delta load successfully UPDATED existing row instead of inserting duplicate!")

        # STEP 5: Verify second ad (DeltaTest_Ad2) remains unchanged
        query3 = f"""
            SELECT ad_name, campaign_name, amount_spent_usd
            FROM {table_name}
            WHERE ad_name = 'DeltaTest_Ad2'
        """
        result3 = snowflake_session.sql(query3).collect()

        assert len(result3) == 1, "Second ad should remain in database"
        assert result3[0]['CAMPAIGN_NAME'] == 'Campaign_Initial2'
        assert float(result3[0]['AMOUNT_SPENT_USD']) == 200.0

    def test_campaign_data_new_column_schema_evolution(self, snowflake_session, test_schema):
        """
        Test that when a new column is added to the dataframe,
        it gets reflected in the database schema.
        """
        schema_name = test_schema['schema_name']
        platform = test_schema['platform']
        table_name = f"{schema_name}.{platform.upper()}_PROCESSED_CAMPAIGN_DATA"

        # STEP 1: Insert initial data without the new column
        initial_data = pd.DataFrame({
            'ad_name': ['SchemaTest_Ad1', 'SchemaTest_Ad2'],
            'campaign_name': ['Campaign_Schema1', 'Campaign_Schema2'],
            'amount_spent_usd': [100.0, 200.0],
            'impressions': [1000, 2000],
            'reach': [500, 1000],
            'wave_number': [1, 1]
        })

        success1, message1 = populate_campaign_data_table(
            initial_data, schema_name, platform, 1, snowflake_session
        )

        assert success1 is True, f"Initial insert failed: {message1}"
        logger.info("STEP 1: Initial data inserted without new column")

        # STEP 2: Insert updated data WITH a new column (e.g., 'quantum_engagement_coefficient')
        updated_data = pd.DataFrame({
            'ad_name': ['SchemaTest_Ad3'],
            'campaign_name': ['Campaign_Schema3'],
            'amount_spent_usd': [300.0],
            'impressions': [3000],
            'reach': [1500],
            'wave_number': [2],
            'quantum_engagement_coefficient': [7.42]  # NEW COLUMN - unique metric
        })

        success2, message2 = populate_campaign_data_table(
            updated_data, schema_name, platform, 2, snowflake_session
        )

        assert success2 is True, f"Insert with new column failed: {message2}"
        logger.info("STEP 2: Data with new column inserted")

        # STEP 3: Check if the new column exists in the schema
        query_columns = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{platform.upper()}_PROCESSED_CAMPAIGN_DATA'
            AND COLUMN_NAME = 'QUANTUM_ENGAGEMENT_COEFFICIENT'
        """
        result_columns = snowflake_session.sql(query_columns).collect()

        logger.info("STEP 3: Checking if new column 'QUANTUM_ENGAGEMENT_COEFFICIENT' exists in schema")
        assert len(result_columns) == 1, "New column 'QUANTUM_ENGAGEMENT_COEFFICIENT' should exist in schema"
        assert result_columns[0]['COLUMN_NAME'] == 'QUANTUM_ENGAGEMENT_COEFFICIENT'
        logger.info("SUCCESS: New column 'QUANTUM_ENGAGEMENT_COEFFICIENT' found in schema")

        # STEP 4: Verify the data in the new column
        query_data = f"""
            SELECT ad_name, quantum_engagement_coefficient
            FROM {table_name}
            WHERE ad_name = 'SchemaTest_Ad3'
        """
        result_data = snowflake_session.sql(query_data).collect()

        assert len(result_data) == 1, "Should find the row with new column data"
        assert float(result_data[0]['QUANTUM_ENGAGEMENT_COEFFICIENT']) == 7.42, "New column should have correct value"
        logger.info("STEP 4: New column data verified successfully")

        # STEP 5: Verify old rows have NULL for the new column
        query_old = f"""
            SELECT ad_name, quantum_engagement_coefficient
            FROM {table_name}
            WHERE ad_name = 'SchemaTest_Ad1'
        """
        result_old = snowflake_session.sql(query_old).collect()

        assert len(result_old) == 1, "Old row should still exist"
        assert result_old[0]['QUANTUM_ENGAGEMENT_COEFFICIENT'] is None, "Old rows should have NULL for new column"
        logger.info("STEP 5: Verified old rows have NULL for new column")
        logger.info("TEST PASSED: Schema evolution with new column works correctly!")


class TestDeltaLoadNamingKeysIntegration:
    """Integration tests for naming keys delta load with real database"""

    def test_naming_keys_latest_overrides_old(self, snowflake_session, test_schema):
        """
        Test that when same ad_set_name is inserted twice, latest data overrides old values.
        """
        schema_name = test_schema['schema_name']
        platform = test_schema['platform']

        # STEP 1: Insert initial data (Wave 1)
        initial_data = pd.DataFrame({
            'ad_set_name': ['DeltaTest_AdSet1', 'DeltaTest_AdSet2'],
            'audience': ['Audience_Original', 'Audience_Original2'],
            'concept': ['Concept_A', 'Concept_B'],
            'position': ['Top', 'Bottom'],
            'wave_number': [1, 1]
        })

        success1, message1 = populate_naming_keys_table(
            initial_data, schema_name, platform, 1, snowflake_session
        )

        assert success1 is True, f"Initial insert failed: {message1}"

        # STEP 2: Verify initial data
        table_name = f"{schema_name}.{platform.upper()}_NAMING_KEYS"
        query1 = f"""
            SELECT ad_set_name, audience, concept, position, wave_number
            FROM {table_name}
            WHERE ad_set_name = 'DeltaTest_AdSet1'
        """
        result1 = snowflake_session.sql(query1).collect()

        logger.info("=" * 80)
        logger.info("BEFORE DELTA LOAD - Initial Data (Wave 1)")
        logger.info("=" * 80)
        assert len(result1) == 1
        row1 = result1[0]
        logger.info(f"ad_set_name: {row1['AD_SET_NAME']}")
        logger.info(f"audience: {row1['AUDIENCE']}")
        logger.info(f"concept: {row1['CONCEPT']}")
        logger.info(f"position: {row1['POSITION']}")
        logger.info(f"wave_number: {row1['WAVE_NUMBER']}")
        logger.info("=" * 80)

        assert row1['AUDIENCE'] == 'Audience_Original'
        assert row1['CONCEPT'] == 'Concept_A'
        assert row1['POSITION'] == 'Top'

        # STEP 3: Delta Load - Update same ad_set_name with new values (Wave 2)
        updated_data = pd.DataFrame({
            'ad_set_name': ['DeltaTest_AdSet1'],  # Same key
            'audience': ['Audience_UPDATED'],  # NEW VALUE
            'concept': ['Concept_UPDATED'],  # NEW VALUE
            'position': ['Middle'],  # NEW VALUE
            'wave_number': [2]
        })

        success2, message2 = populate_naming_keys_table(
            updated_data, schema_name, platform, 2, snowflake_session
        )

        assert success2 is True, f"Delta load failed: {message2}"

        # STEP 4: Verify old values are REPLACED (not added as new row)
        query2 = f"""
            SELECT ad_set_name, audience, concept, position, wave_number
            FROM {table_name}
            WHERE ad_set_name = 'DeltaTest_AdSet1'
        """
        result2 = snowflake_session.sql(query2).collect()

        logger.info("")
        logger.info("=" * 80)
        logger.info("AFTER DELTA LOAD - Updated Data (Wave 2)")
        logger.info("=" * 80)
        # Should be only 1 row (updated, not duplicated)
        assert len(result2) == 1, "Delta load should UPDATE, not INSERT duplicate"
        logger.info(f"Number of rows: {len(result2)} (should be 1 - proving UPDATE not INSERT)")

        row2 = result2[0]
        logger.info(f"ad_set_name: {row2['AD_SET_NAME']}")
        logger.info(f"audience: {row2['AUDIENCE']} (was 'Audience_Original')")
        logger.info(f"concept: {row2['CONCEPT']} (was 'Concept_A')")
        logger.info(f"position: {row2['POSITION']} (was 'Top')")
        logger.info(f"wave_number: {row2['WAVE_NUMBER']} (was 1)")
        logger.info("=" * 80)

        # Verify new values have OVERRIDDEN old values
        assert row2['AUDIENCE'] == 'Audience_UPDATED', "Audience should be updated"
        assert row2['CONCEPT'] == 'Concept_UPDATED', "Concept should be updated"
        assert row2['POSITION'] == 'Middle', "Position should be updated"
        assert int(row2['WAVE_NUMBER']) == 2, "Wave should be updated"

        logger.info("TEST PASSED: Delta load successfully UPDATED existing row instead of inserting duplicate!")

    def test_naming_keys_new_column_schema_evolution(self, snowflake_session, test_schema):
        """
        Test that when a new column is added to the naming keys dataframe,
        it gets reflected in the database schema.
        """
        schema_name = test_schema['schema_name']
        platform = test_schema['platform']
        table_name = f"{schema_name}.{platform.upper()}_NAMING_KEYS"

        # STEP 1: Insert initial data without the new column
        initial_data = pd.DataFrame({
            'ad_set_name': ['SchemaTest_AdSet1', 'SchemaTest_AdSet2'],
            'audience': ['Audience_Schema1', 'Audience_Schema2'],
            'concept': ['Concept_X', 'Concept_Y'],
            'position': ['Top', 'Bottom'],
            'wave_number': [1, 1]
        })

        success1, message1 = populate_naming_keys_table(
            initial_data, schema_name, platform, 1, snowflake_session
        )

        assert success1 is True, f"Initial insert failed: {message1}"
        logger.info("STEP 1: Initial data inserted without new column")

        # STEP 2: Insert updated data WITH a new column (e.g., 'neuro_resonance_index')
        updated_data = pd.DataFrame({
            'ad_set_name': ['SchemaTest_AdSet3'],
            'audience': ['Audience_Schema3'],
            'concept': ['Concept_Z'],
            'position': ['Middle'],
            'wave_number': [2],
            'neuro_resonance_index': ['Alpha-Theta-9.3']  # NEW COLUMN - unique metric
        })

        success2, message2 = populate_naming_keys_table(
            updated_data, schema_name, platform, 2, snowflake_session
        )

        assert success2 is True, f"Insert with new column failed: {message2}"
        logger.info("STEP 2: Data with new column inserted")

        # STEP 3: Check if the new column exists in the schema
        query_columns = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{platform.upper()}_NAMING_KEYS'
            AND COLUMN_NAME = 'NEURO_RESONANCE_INDEX'
        """
        result_columns = snowflake_session.sql(query_columns).collect()

        logger.info("STEP 3: Checking if new column 'NEURO_RESONANCE_INDEX' exists in schema")
        assert len(result_columns) == 1, "New column 'NEURO_RESONANCE_INDEX' should exist in schema"
        assert result_columns[0]['COLUMN_NAME'] == 'NEURO_RESONANCE_INDEX'
        logger.info("SUCCESS: New column 'NEURO_RESONANCE_INDEX' found in schema")

        # STEP 4: Verify the data in the new column
        query_data = f"""
            SELECT ad_set_name, neuro_resonance_index
            FROM {table_name}
            WHERE ad_set_name = 'SchemaTest_AdSet3'
        """
        result_data = snowflake_session.sql(query_data).collect()

        assert len(result_data) == 1, "Should find the row with new column data"
        assert result_data[0]['NEURO_RESONANCE_INDEX'] == 'Alpha-Theta-9.3', "New column should have correct value"
        logger.info("STEP 4: New column data verified successfully")

        # STEP 5: Verify old rows have NULL for the new column
        query_old = f"""
            SELECT ad_set_name, neuro_resonance_index
            FROM {table_name}
            WHERE ad_set_name = 'SchemaTest_AdSet1'
        """
        result_old = snowflake_session.sql(query_old).collect()

        assert len(result_old) == 1, "Old row should still exist"
        assert result_old[0]['NEURO_RESONANCE_INDEX'] is None, "Old rows should have NULL for new column"
        logger.info("STEP 5: Verified old rows have NULL for new column")
        logger.info("TEST PASSED: Schema evolution with new column works correctly for naming keys!")

