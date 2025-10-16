#!/bin/bash
set -e  # Exit on error

echo "🚀 Starting Streamlit deployment to Snowflake..."

# Configure SnowCLI connection via config file (avoids interactive prompts)
echo "📝 Configuring Snowflake connection..."

# Use PUBLIC as default schema if not set
SCHEMA="${SNOWFLAKE_SCHEMA:-PUBLIC}"

# Create config file with key-pair authentication
mkdir -p ~/.snowflake

# Process and convert private key to PKCS#8 format
echo "🔑 Processing private key..."

# Save the private key to a temporary file
echo "${SNOWFLAKE_PRIVATE_KEY_RAW}" > /tmp/snowflake_rsa_key.pem
chmod 600 /tmp/snowflake_rsa_key.pem

# Convert to PKCS#8 format (required by SnowCLI)
echo "Converting private key to PKCS#8 format..."
openssl pkcs8 -topk8 -inform PEM -outform PEM \
  -in /tmp/snowflake_rsa_key.pem \
  -out /tmp/snowflake_key.p8 \
  -nocrypt

chmod 600 /tmp/snowflake_key.p8

# Read the converted key content
PKCS8_KEY=$(cat /tmp/snowflake_key.p8)

# Create or update config.toml with JWT authentication using PKCS#8 key
cat >> ~/.snowflake/config.toml << EOF

[connections.deploy]
account = "${SNOWFLAKE_ACCOUNT}"
user = "${SNOWFLAKE_USER}"
authenticator = "SNOWFLAKE_JWT"
private_key_raw = """${PKCS8_KEY}"""
warehouse = "${SNOWFLAKE_WAREHOUSE}"
EOF

# Add database only if specified
if [ -n "${SNOWFLAKE_DATABASE}" ]; then
  echo "database = \"${SNOWFLAKE_DATABASE}\"" >> ~/.snowflake/config.toml
fi

# Add schema only if specified
if [ -n "${SCHEMA}" ]; then
  echo "schema = \"${SCHEMA}\"" >> ~/.snowflake/config.toml
fi

# Add role only if specified
if [ -n "${SNOWFLAKE_ROLE}" ]; then
  echo "role = \"${SNOWFLAKE_ROLE}\"" >> ~/.snowflake/config.toml
fi

# Set secure permissions (required by SnowCLI)
chmod 0600 ~/.snowflake/config.toml

echo "✅ Connection configured with PKCS#8 key-pair authentication"

# Test connection
echo "🔌 Testing connection..."
snow connection test --connection deploy

# Build fully qualified names
DB_PREFIX=""
if [ -n "${SNOWFLAKE_DATABASE}" ]; then
  DB_PREFIX="${SNOWFLAKE_DATABASE}."
fi

STAGE_NAME="${DB_PREFIX}${SCHEMA}.streamlit_stage"
APP_NAME="${DB_PREFIX}${SCHEMA}.SPARK_CAMPAIGN_PROCESSOR"

# Create stage if not exists
echo "📦 Creating/verifying stage..."
echo "Using stage: ${STAGE_NAME}"
snow sql --connection deploy -q "CREATE STAGE IF NOT EXISTS ${STAGE_NAME}"

# Upload files
echo "📤 Uploading application files..."
FILES=(
  "streamlit_app.py"
  "environment.yml"
  "data_processor.py"
  "snowflake_operations.py"
  "snowpark_connection.py"
  "validators.py"
  "sql_templates.py"
  "constants.py"
)

for file in "${FILES[@]}"; do
  echo "  Uploading $file..."
  snow stage copy --connection deploy "$file" @${STAGE_NAME}/ --overwrite
done

# Verify uploads
echo "✅ Verifying uploads..."
snow stage list-files --connection deploy @${STAGE_NAME}

# Create or recreate Streamlit app
echo "🎯 Deploying Streamlit app..."
echo "Using app name: ${APP_NAME}"
snow sql --connection deploy -q "DROP STREAMLIT IF EXISTS ${APP_NAME}"
snow sql --connection deploy -q "CREATE STREAMLIT ${APP_NAME} FROM '@${STAGE_NAME}' MAIN_FILE = 'streamlit_app.py' QUERY_WAREHOUSE = ${SNOWFLAKE_WAREHOUSE}"

# Get app URL
echo "🌐 Getting app URL..."
snow streamlit get-url --connection deploy ${APP_NAME}

# Cleanup temporary key files for security
echo "🧹 Cleaning up temporary files..."
rm -f /tmp/snowflake_rsa_key.pem /tmp/snowflake_key.p8

echo "✅ Deployment completed successfully!"