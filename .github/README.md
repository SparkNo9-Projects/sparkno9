# CI/CD Pipeline Documentation

## Streamlit Deployment Pipeline

Automated deployment of Streamlit app to Snowflake using GitHub Actions.

## Setup

### 1. Configure GitHub Secrets

Add these secrets to your repository (`Settings` → `Secrets and variables` → `Actions`):

| Secret Name | Required | Description | Example |
|------------|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | ✅ Yes | Snowflake account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | ✅ Yes | Snowflake username (service user) | `CICD_USER` |
| `SNOWFLAKE_PRIVATE_KEY_RAW` | ✅ Yes | Private key (single-line format with `\n`) | `-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----` |
| `SNOWFLAKE_WAREHOUSE` | ✅ Yes | Warehouse name | `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | ✅ Yes | Database name | `SPARK_NO_9` |
| `SNOWFLAKE_SCHEMA` | ✅ Yes | Schema name | `PUBLIC` |
| `PRIVATE_KEY_PASSPHRASE` | ⚠️ If encrypted | Passphrase for encrypted private key | `your_passphrase` |

**Note:** This deployment uses **key-pair authentication** (not password). The `SNOWFLAKE_PRIVATE_KEY_RAW` secret should contain your private key in single-line format with literal `\n` characters for line breaks.

**To convert your private key to single-line format:**
- **Linux/Mac:** `cat rsa_key.pem | tr '\n' '\\n' && echo`
- **Windows PowerShell:** `(Get-Content rsa_key.pem -Raw) -replace "`r`n", "\n" -replace "`n", "\n"`

### 2. Deployment Triggers

The pipeline runs automatically when:
- **Push to `main` or `dev` branch** with changes in `src/project_spark/streamlit/`
- **Manual trigger** via GitHub Actions UI (allows environment selection)

### 3. Manual Deployment

1. Go to **Actions** tab in GitHub
2. Select **Deploy Streamlit to Snowflake** workflow
3. Click **Run workflow**
4. Select environment (dev/prod)
5. Click **Run workflow**

## Pipeline Steps

1. **Checkout code** - Gets latest code
2. **Setup Python** - Installs Python 3.11
3. **Install SnowCLI** - Installs Snowflake CLI
4. **Deploy** - Runs deployment script:
   - Configures Snowflake connection
   - Creates stage
   - Uploads all app files
   - Creates/updates Streamlit app
5. **Verify** - Confirms deployment success

## Files Deployed

- `streamlit_app.py` - Main Streamlit application
- `environment.yml` - Python dependencies for Snowflake
- `snowflake_operations.py` - Snowflake database operations
- `data_processor.py` - Data transformation and processing
- `sql_templates.py` - SQL query templates
- `snowpark_connection.py` - Snowpark session management
- `constants.py` - Application constants and configuration
- `validators.py` - Input validation utilities

## Snowflake Setup

### Prerequisites for Key-Pair Authentication

Before configuring GitHub secrets, you must set up a Snowflake service user with key-pair authentication.


#### How to Run SQL Commands in Snowflake

1. **Log into Snowflake**: Open your Snowflake account in a web browser
2. **Open a Worksheet**: Click on "Worksheets" in the left sidebar (or click the "+" button at the top), then select "SQL Worksheet"

   ![Snowflake Worksheets Navigation](../assets/images/snowflake_worksheets_navigation.png)

3. **Copy SQL commands**: Copy the SQL commands from each section below
4. **Paste into worksheet**: Paste them into your Snowflake worksheet
5. **Run the commands**: Click the "Run" button (▶) at the top right or press `Ctrl+Enter` (Windows) / `Cmd+Enter` (Mac)

   ![Snowflake SQL Worksheet - Create User](../assets/images/snowflake_sql_worksheet_create_user.png)

You can either run each section separately, or combine all sections into one SQL file and run them together.

#### 1. Create Service User
```sql
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS CICD_USER
  TYPE = SERVICE
  COMMENT = 'Service user for GitHub Actions CI/CD deployments';
```

#### 2. Generate Key Pair

**Linux/Mac:**
```bash
# Generate private key
openssl genrsa -out rsa_key.pem 2048

# Generate public key
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
```

**Windows PowerShell:**
```powershell
# Install OpenSSL if needed: choco install openssl
openssl genrsa -out rsa_key.pem 2048
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
```

#### 3. Register Public Key
```sql
USE ROLE ACCOUNTADMIN;

-- Remove BEGIN/END headers and line breaks from public key
ALTER USER CICD_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';

-- Verify
DESCRIBE USER CICD_USER;
```

#### 4. Create Database and Warehouse

```sql
USE ROLE ACCOUNTADMIN;

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS SPARK_NO_9
  COMMENT = 'Database for Spark Campaign Processor application';

-- Verify creation
SHOW DATABASES LIKE 'SPARK_NO_9';
```

#### 5. Grant Permissions
```sql
USE ROLE ACCOUNTADMIN;

-- Grant SYSADMIN role to user
GRANT ROLE SYSADMIN TO USER CICD_USER;
ALTER USER CICD_USER SET DEFAULT_ROLE = SYSADMIN;

-- Database permissions
GRANT USAGE ON DATABASE SPARK_NO_9 TO ROLE SYSADMIN;
GRANT MONITOR ON DATABASE SPARK_NO_9 TO ROLE SYSADMIN;
GRANT CREATE SCHEMA ON DATABASE SPARK_NO_9 TO ROLE SYSADMIN;

-- Schema permissions
GRANT USAGE ON SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;
GRANT CREATE STREAMLIT ON SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;
GRANT CREATE STAGE ON SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;
GRANT CREATE VIEW ON SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;

-- Warehouse permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
GRANT MODIFY ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

-- Future grants (automatically apply to new objects)
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE STREAMLITS IN SCHEMA SPARK_NO_9.PUBLIC TO ROLE SYSADMIN;

-- Set user defaults
ALTER USER CICD_USER SET DEFAULT_ROLE = SYSADMIN;
ALTER USER CICD_USER SET DEFAULT_WAREHOUSE = COMPUTE_WH;
ALTER USER CICD_USER SET DEFAULT_NAMESPACE = "SPARK_NO_9"."PUBLIC";

-- Grant database ownership (optional - for full control)
GRANT OWNERSHIP ON DATABASE SPARK_NO_9 TO ROLE SYSADMIN COPY CURRENT GRANTS;

-- Verify grants
SHOW GRANTS TO USER CICD_USER;
```

**Security Best Practices:**
- ✅ Use `TYPE = SERVICE` for automation users
- ✅ Store private keys only in GitHub Secrets
- ✅ Never commit private keys to git
- ✅ Rotate keys every 90 days
- ✅ Use principle of least privilege for roles

## Troubleshooting

### Pipeline fails at "Test connection"
- Verify all secrets are correctly set
- Check Snowflake account identifier format
- Ensure user has required permissions

### Pipeline fails at "Upload files"
- Check if stage exists in Snowflake
- Verify warehouse is running
- Check role permissions for stage creation

### Pipeline fails at "Deploy Streamlit app"
- Ensure warehouse has compute resources
- Check if app name conflicts exist
- Verify all files uploaded successfully

