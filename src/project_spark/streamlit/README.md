# Spark No. 9 - Campaign Data Processor

A Streamlit application for uploading and processing campaign data files with standardized naming conventions, deployed in Snowflake.

## Overview

This application provides a user-friendly interface for processing campaign data files in Snowflake. It automatically creates client schemas, processes CSV files, and stores data in structured tables for analysis.

## Features

- **File Upload Interface**: Upload campaign data and naming key CSV files
- **Automatic Schema Creation**: Creates client-specific schemas in Snowflake
- **Data Processing**: Processes and validates uploaded files
- **Results Dashboard**: Shows processing status and data quality metrics
- **Multi-Platform Support**: Supports Meta, LinkedIn, Google, TikTok, Twitter, and Snapchat

## Setup

### Prerequisites

- Snowflake account with appropriate permissions
- Access to create schemas and tables
- CSV files with campaign data and naming keys
- SnowCLI installed and configured

### Installation

1. Deploy the Streamlit app to your Snowflake account using SnowCLI
2. Ensure you have the required permissions to create schemas and tables
3. Access the app through Snowsight

## Deployment Instructions

### Initial Deployment

1. **Configure SnowCLI Connection**:
   ```bash
   snow connection add --connection-name test --account "your_account" --user "your_user" --password "your_password" --warehouse "COMPUTE_WH" --database "SPARK_NO_9_TEST" --schema "PUBLIC" --role "ACCOUNTADMIN"
   ```

2. **Create Stage** (if not exists):
   ```bash
   snow sql --connection test -q "CREATE STAGE IF NOT EXISTS streamlit_stage"
   ```

3. **Upload Files to Stage**:
   ```bash
   snow stage copy --connection test streamlit_app.py @streamlit_stage/
   snow stage copy --connection test environment.yml @streamlit_stage/
   snow stage copy --connection test requirements.txt @streamlit_stage/
   ```

4. **Create Streamlit App**:
   ```bash
   snow sql --connection test -q "CREATE STREAMLIT SPARK_CAMPAIGN_PROCESSOR FROM '@streamlit_stage' QUERY_WAREHOUSE = COMPUTE_WH"
   ```

5. **Get App URL**:
   ```bash
   snow streamlit get-url --connection test SPARK_CAMPAIGN_PROCESSOR
   ```

### Updating the App

To update the Streamlit app after making changes:

1. **Upload Updated Files**:
   ```bash
   snow stage copy --connection test streamlit_app.py @streamlit_stage/ --overwrite
   snow stage copy --connection test environment.yml @streamlit_stage/ --overwrite
   ```

2. **Recreate the App** (if needed):
   ```bash
   snow streamlit drop --connection test SPARK_CAMPAIGN_PROCESSOR
   snow sql --connection test -q "CREATE STREAMLIT SPARK_CAMPAIGN_PROCESSOR FROM '@streamlit_stage' QUERY_WAREHOUSE = COMPUTE_WH"
   ```

3. **Verify Deployment**:
   ```bash
   snow streamlit list --connection test
   ```

## Usage

### 1. Upload Files

Upload two CSV files:
- **Campaign Data**: Contains campaign performance metrics
- **Naming Key**: Contains campaign naming conventions and mappings

### 2. Enter Metadata

Provide the following information:
- **Client**: Client name (e.g., caterpillar)
- **Platform**: Advertising platform (meta, linkedin, etc.)
- **Year**: Campaign year (e.g., 2024)
- **Wave**: Wave number (1, 2, 3, etc.)

### 3. Process Files

Click "Process Files" to:
- Create client schema if it doesn't exist
- Process and validate the uploaded files
- Insert data into Snowflake tables
- Generate processing logs

### 4. View Results

The application displays:
- Processing status and metrics
- Data quality information
- Next steps and recommendations

## Data Structure

### Generated Schema

The application creates a schema with the naming convention:
```
CLIENT_{CLIENT}_{YEAR}
```

Example: `CLIENT_CATERPILLAR_2024`

Tables are prefixed with the platform name within the schema:
- `META_NAMING_KEYS`
- `META_PROCESSED_CAMPAIGN_DATA`
- `LINKEDIN_NAMING_KEYS`
- `LINKEDIN_PROCESSED_CAMPAIGN_DATA`

### Tables Created

1. **naming_keys**: Campaign naming conventions and mappings
2. **processed_campaign_data**: Campaign performance data
3. **processing_log**: Processing history and audit trail

## File Formats

### Campaign Data CSV

Expected columns (flexible schema):
- `ad_set_name`: Campaign ad set identifier
- `ad_name`: Individual ad name
- `campaign_name`: Campaign name
- `impressions`: Number of impressions
- `results`: Campaign results/conversions
- `amount_spent_usd`: Amount spent in USD

### Naming Key CSV

Expected columns:
- `ad_set_name`: Campaign ad set identifier
- `audience`: Target audience
- `concept`: Campaign concept
- `position`: Ad position
- `ad_descriptor`: Ad description
- `ad_direction`: Ad direction/type

## Supported Platforms

- Meta (Facebook/Instagram)
- LinkedIn
- Google
- TikTok
- Twitter
- Snapchat

## Error Handling

The application includes comprehensive error handling for:
- Invalid file formats
- Missing required columns
- Database connection issues
- Schema creation failures
- Data processing errors

## Security

- Uses Snowflake's built-in security model
- Schema-level data isolation
- Role-based access control
- Secure file processing

## Troubleshooting

### Common Issues

1. **File Upload Errors**
   - Ensure CSV files are properly formatted
   - Check for required columns
   - Verify file encoding (UTF-8)

2. **Schema Creation Failures**
   - Verify database permissions
   - Check for existing schemas
   - Ensure warehouse is running

3. **Processing Errors**
   - Review file formats and data types
   - Check for duplicate entries
   - Verify naming conventions

### Getting Help

For additional support:
1. Check the processing logs in the `processing_log` table
2. Review error messages in the application
3. Contact your Snowflake administrator

## Streamlit in Snowflake Best Practices

### Code Structure

- **Main App File**: `streamlit_app.py` (required)
- **Dependencies**: `environment.yml` (required)
- **Additional Packages**: `requirements.txt` (optional)
- **Configuration**: Use Snowflake's built-in session management

### Snowflake Integration

```python
# Use official Snowflake session pattern
from snowflake.snowpark.context import get_active_session

def get_snowflake_session():
    try:
        return get_active_session()
    except Exception as e:
        st.error(f"Failed to get Snowflake session: {e}")
        return None
```

### Environment Configuration

The `environment.yml` file must follow Snowflake's requirements:

```yaml
name: spark-campaign-processor
channels:
  - snowflake  # Required: Only use snowflake channel
dependencies:
  - python=3.8  # Use supported Python version
  - streamlit   # Don't pin streamlit version
  - pandas      # Add other packages as needed
```

### Supported Python Versions

- **Python 3.8**: Supported by all packages
- **Python 3.9**: Supported by most packages
- **Python 3.10**: Limited support
- **Python 3.11**: Supported by most packages
- **Python 3.12**: Supported by most packages

### Unsupported Features

The following Streamlit features are **NOT** supported in Snowflake:

- `st.bokeh_chart`
- `st.set_page_config` (with `page_title`, `page_icon`, `menu_items`)
- `st.components.v1.iframe`
- `st.components.v1.declare_component`
- Anchor links
- External script loading (CSP restrictions)

### Security Considerations

- **Content Security Policy**: All external resources are blocked
- **Mapbox Support**: Only Mapbox tiles are allowed for maps
- **Database Security**: Use parameterized queries
- **Role-Based Access**: Implement proper RBAC

### Performance Optimization

- Use `@st.cache_data` for expensive computations
- Use `@st.cache_resource` for database connections
- Implement proper session state management
- Optimize SQL queries for Snowflake

## Technical Details

### Architecture

- **Frontend**: Streamlit web interface
- **Backend**: Snowflake database and processing
- **Storage**: Snowflake tables and stages
- **Security**: Snowflake RBAC and data governance

### Performance

- Optimized for large CSV files
- Efficient batch processing
- Minimal memory usage
- Fast schema creation

## Troubleshooting

### Common Deployment Issues

1. **Python Version Conflicts**
   - Use Python 3.8 for maximum compatibility
   - Check package compatibility matrix

2. **Package Installation Failures**
   - Only use packages from Snowflake channel
   - Avoid external Anaconda channels

3. **Session Connection Issues**
   - Use `get_active_session()` pattern
   - Implement proper error handling

4. **File Upload Problems**
   - Ensure files are uploaded to stage first
   - Use `--overwrite` flag for updates

### Debugging Commands

```bash
# Check app status
snow streamlit list --connection test

# View app details
snow streamlit describe --connection test SPARK_CAMPAIGN_PROCESSOR

# Test connection
snow connection test --connection test

# View stage contents
snow stage list-files --connection test @streamlit_stage
```

## Version History

- **v1.0**: Initial release with basic file processing
- **v1.1**: Added multi-platform support
- **v1.2**: Enhanced error handling and logging
- **v1.3**: Added SnowCLI deployment instructions and best practices

## License

This application is part of the Spark No. 9 project and follows the project's licensing terms.
