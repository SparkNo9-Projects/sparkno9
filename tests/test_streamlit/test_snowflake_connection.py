"""
Simple unit test for Snowflake connection.

Prerequisites:
- Set up .env file with Snowflake credentials or configure Streamlit secrets
- Requires SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
"""

import pytest
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

from project_spark.streamlit.snowflake_operations import get_snowflake_connection


def test_snowflake_connection():
    """Test that we can successfully connect to Snowflake"""
    logger.info("Attempting to connect to Snowflake...")

    session = get_snowflake_connection()

    assert session is not None, "Failed to connect to Snowflake."

    logger.info("Successfully connected to Snowflake!")

    # Close the connection
    session.close()
    logger.info("Connection closed successfully")
