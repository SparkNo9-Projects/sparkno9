"""
Unified Snowpark connection for both local development and Snowflake deployment.
This replaces the dual Snowpark/SQLAlchemy approach with Snowpark-only.

Supports both password and RSA key pair authentication.
"""

import os
import logging
from typing import Optional
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

logger = logging.getLogger(__name__)


def load_private_key(private_key_path: str, private_key_passphrase: Optional[str] = None):
    """
    Load RSA private key from file.

    Args:
        private_key_path: Path to the private key file
        private_key_passphrase: Optional passphrase for encrypted private key

    Returns:
        Private key bytes in DER format
    """
    try:
        with open(private_key_path, 'rb') as key_file:
            if private_key_passphrase:
                passphrase_bytes = private_key_passphrase.encode()
            else:
                passphrase_bytes = None

            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase_bytes,
                backend=default_backend()
            )

            # Convert to DER format (required by Snowflake)
            private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            logger.info(f"Successfully loaded private key from {private_key_path}")
            return private_key_bytes

    except Exception as e:
        logger.error(f"Failed to load private key from {private_key_path}: {e}")
        raise


def get_snowpark_session() -> Optional[Session]:
    """
    Get Snowpark session for both local development and Snowflake deployment.
    
    Priority order:
    1. Active session (when running in Streamlit-in-Snowflake)
    2. New session from environment variables (for local development)
    3. New session from Streamlit secrets (for local Streamlit testing)
    
    Returns:
        Snowpark Session or None if connection fails
    """
    # Try to get active session first (Streamlit-in-Snowflake)
    try:
        session = get_active_session()
        logger.info("Using active Snowpark session (deployed in Snowflake)")
        return session
    except:
        logger.info("No active session found, creating new Snowpark session for local development")
    
    # Create new session for local development
    try:
        # Try Streamlit secrets first
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
                config = st.secrets['snowflake']
                connection_parameters = {
                    "account": config.get("account"),
                    "user": config.get("user"),
                    "password": config.get("password"),
                    "warehouse": config.get("warehouse"),
                    "database": config.get("database"),
                    "schema": config.get("schema", "PUBLIC"),
                    "role": config.get("role")
                }
                logger.info("Creating Snowpark session from Streamlit secrets")
                session = Session.builder.configs(connection_parameters).create()
                return session
        except (ImportError, FileNotFoundError, KeyError) as e:
            logger.debug(f"Streamlit secrets not available: {e}")
        
        # Fall back to environment variables
        from dotenv import load_dotenv
        load_dotenv()

        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

        connection_parameters = {
            "account": account,
            "user": user,
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
            "database": os.getenv('SNOWFLAKE_DATABASE'),
            "schema": os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            "role": os.getenv('SNOWFLAKE_ROLE')
        }

        # Validate required parameters
        if not all([account, user]):
            logger.error("Missing required Snowflake connection parameters (account, user)")
            return None

        # Determine authentication method: RSA key pair (preferred) or password
        if private_key_path and os.path.exists(private_key_path):
            # RSA Key Pair Authentication
            logger.info("Using RSA key pair authentication")
            try:
                private_key_bytes = load_private_key(private_key_path, private_key_passphrase)
                connection_parameters["private_key"] = private_key_bytes
                logger.info("Creating Snowpark session with RSA key pair authentication")
            except Exception as e:
                logger.error(f"Failed to load private key, falling back to password auth: {e}")
                if not password:
                    logger.error("No password available as fallback")
                    return None
                connection_parameters["password"] = password
        elif password:
            # Password Authentication
            logger.info("Using password authentication")
            connection_parameters["password"] = password
        else:
            logger.error("No authentication method available (need either private key or password)")
            return None

        session = Session.builder.configs(connection_parameters).create()
        return session
        
    except Exception as e:
        logger.error(f"Failed to create Snowpark session: {e}")
        return None


def test_snowpark_connection(session: Session) -> bool:
    """
    Test the Snowpark session connection.
    
    Args:
        session: Snowpark Session to test
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        result = session.sql("SELECT CURRENT_VERSION()").collect()
        version = result[0][0]
        logger.info(f"Snowpark connection successful. Snowflake version: {version}")
        return True
    except Exception as e:
        logger.error(f"Snowpark connection test failed: {e}")
        return False

