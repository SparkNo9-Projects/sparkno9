# Spark No. 9 - Client Onboarding System

**Data Automation & AI Readiness Pilot for Spark No. 9**

A Python-based system for creating and managing client platform schemas in Snowflake, supporting multi-wave campaigns with column variations. This repository contains all source code, technical documentation, and development artifacts for Project Spark.

## 🚀 Quick Start

```bash
# Clone and setup
git clone <repository-url>
cd cp_spark

# Install dependencies with uv
uv sync

# Configure environment
cp .env.example .env
# Edit .env with your Snowflake credentials

# Run the Streamlit app
uv run streamlit run app.py
```

## 🔗 Quick Links

  * [Project Google Drive](https://drive.google.com/drive/u/0/folders/0ADbWAGo29PpcUk9PVA)
  * **Project Board (Jira):**

-----

## 📂 Repository Structure

This repository follows a scalable, industry-standard structure for Python applications. It is the single source of truth for all executable and technical assets.

```
cp_spark/
├── src/project_spark/
│   ├── database/           # Database models and connections
│   │   ├── models.py       # SQLAlchemy table definitions
│   │   └── database.py     # Snowflake connection management
│   ├── ingestion/          # Data ingestion modules
│   ├── processing/         # Data cleaning and transformation
│   ├── storage/            # Data output/sinks
│   ├── utils/              # Shared utility functions
│   ├── main.py             # Main application entry point
│   └── config.py           # Configuration management
├── examples/
│   └── create_client_schema.py # Usage examples
├── tests/
│   ├── integration/        # Integration tests
│   └── unit/               # Unit tests
├── config/                 # Environment configurations
├── docs/                   # Technical documentation
├── notebooks/              # Jupyter notebooks for exploration
├── pyproject.toml          # Project configuration and dependencies
└── .env.example            # Environment configuration template
```

## 📋 Features

- **Streamlit Web Interface**: User-friendly file upload with standardized naming
- **Automated Schema Creation**: Creates complete client schemas with all required tables
- **Multi-Wave Campaign Support**: Handles campaigns with varying column structures
- **Direct Data Processing**: Immediate file processing upon upload
- **Role-Based Access Control**: Automatically creates roles and permissions
- **File Upload Stages**: Sets up Snowflake stages for data uploads
- **CSV Data Validation**: Foreign key relationship validation and data integrity checks
- **Processing Logs**: Comprehensive activity tracking and debugging
- **Type Safety**: Full Python typing with Pydantic validation

### Key Components

  * **`database/`**: SQLAlchemy models and Snowflake connection management
  * **`ingestion/`**: Data ingestion from files and APIs
  * **`processing/`**: Data cleaning and transformation logic
  * **`storage/`**: Data output to various destinations
  * **`config/`**: Environment-specific configurations
  * **`tests/`**: Comprehensive unit and integration tests

-----

## 🚀 Getting Started (Local Setup)

Follow these steps to set up your local development environment.

**Prerequisites:**

  * Python 3.12+
  * Git

**Installation:**

1.  **Clone the repository:**

    ```bash
    git clone [repository_url]
    cd project-spark
    ```

2.  **Create and activate a virtual environment:**

    ```bash
    # For macOS/Linux
    python3 -m venv .venv
    source .venv/bin/activate

    # For Windows
    python -m venv .venv
    .\.venv\Scripts\activate
    ```

3.  **Install dependencies with uv:**

    ```bash
    # Install uv if you haven't already
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Install project dependencies
    uv sync
    ```

4.  **Set up local configuration:**

      * Create a copy of `config/development.yml` and name it `config/local.yml`.
      * Update `config/local.yml` with any necessary local values (e.g., database paths, API keys).
      * **IMPORTANT:** `config/local.yml` is listed in `.gitignore` and must **never** be committed to the repository.

-----

## 📖 Usage

### Streamlit File Upload App

Run the web-based file upload interface:

```bash
# Start the Streamlit app
uv run streamlit run app.py
```

This launches a user-friendly web interface at `http://localhost:8501` where you can:
- Upload CSV files with drag-and-drop
- Enter metadata (client, platform, year, wave number)
- Automatically rename and save files with standardized nomenclature
- Preview data before saving
- View all existing files

See the [Streamlit App Documentation](examples/README.md#streamlit-file-upload-app) for more details.

### Command Line
```bash
# Create schemas for multiple clients
uv run spark-create-schema

# Run schema creation example
uv run python examples/create_client_schema.py

# Test event-driven processing flow
uv run python examples/test_event_driven_flow.py
```

### Python API
See the complete example in [`examples/test_event_driven_flow.py`](examples/test_event_driven_flow.py) which demonstrates:
- Client schema creation
- Event-driven processing setup
- File upload and automatic processing
- CSV data validation
- Real-world workflow with BYD Meta campaign data

## 🏗️ Schema Structure

Each client schema contains **3 tables + 1 stage** (simplified architecture):

```
CLIENT_{NAME}_{PLATFORM}_{YEAR}/
├── naming_keys              # Campaign naming conventions
├── processed_campaign_data  # Final processed metrics  
├── processing_log           # Complete audit trail
└── {schema}_stage          # File upload stage (backup & reference)
```

**Processing Log** tracks all file processing with:
- Timestamp, wave number, filename
- File hash (duplicate prevention)
- Column count, row count, column names
- Processing status and error messages

**Stage** provides:
- File backup and reference
- Original CSV files preserved
- Query capabilities for reprocessing

## 🔧 API Reference

### `ClientSchemaManager`

#### `create_client_platform_schema(client_name, platform, project_year)`
Creates a complete schema for a client platform combination with 3 tables.

**Parameters:**
- `client_name`: Name of the client (e.g., "caterpillar")
- `platform`: Platform type (e.g., "meta", "linkedin")
- `project_year`: Project year (e.g., "2024")

**Returns:** Success/error message string

#### `upload_and_process_file(file_path, schema_name, wave_number)`
Processes a CSV file directly to the appropriate table and logs to processing_log.

**Parameters:**
- `file_path`: Path to CSV file
- `schema_name`: Target schema name
- `wave_number`: Wave number for this upload

**Returns:** Success/error message string

**Features:**
- Automatic schema evolution (adds new columns)
- File hash calculation for duplicate prevention
- Complete audit logging

#### `list_client_schemas()`
Returns list of all existing client schemas.

#### `grant_schema_access(schema_name, user_email)`
Grants schema access to a user.

#### `drop_client_schema(client_name, platform, project_year)`
⚠️ **Destructive operation** - Drops entire schema and role.

## ⚙️ Environment Variables

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_ROLE=your_role
```

-----

## 🌿 Development Workflow (GitHub Flow)

We follow the **GitHub Flow** branching model. The rules are simple but mandatory.

1.  **`main` is sacred.** The `main` branch must always be stable and deployable. Direct pushes to `main` are disabled.
2.  **Branch for everything.** All new work must be done on a new feature branch.
      * **Branch Naming:** `feature/[short-description]` (e.g., `feature/linkedin-parser`)
3.  **Use Pull Requests (PRs).** When your work is complete, open a Pull Request to merge your feature branch into `main`.
4.  **Require Code Review.** Every PR must be reviewed and approved by at least one other team member before it can be merged. This is our primary mechanism for ensuring code quality and sharing knowledge.

## 🔧 Development

### Install development dependencies
```bash
uv sync --extra dev
```

### Run tests
```bash
uv run pytest
```

### Code formatting
```bash
uv run black .
uv run isort .
```

### Type checking
```bash
uv run mypy src/
```

## 🌟 Why uv?

This project uses [uv](https://github.com/astral-sh/uv) for dependency management:

- **Fast**: 10-100x faster than pip
- **Reliable**: Deterministic builds with lock files
- **Simple**: Drop-in replacement for pip with better UX
- **Modern**: Built with Rust for performance

## 🤝 Contributing

1. Install dependencies: `uv sync --extra dev`
2. Make your changes
3. Run tests: `uv run pytest`
4. Format code: `uv run black . && uv run isort .`
5. Submit a pull request

-----