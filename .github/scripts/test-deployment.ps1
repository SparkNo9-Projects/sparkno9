# PowerShell deployment test script for Windows
# Use this to test deployment locally on Windows

Write-Host "🚀 Testing Streamlit deployment configuration..." -ForegroundColor Green

# Check required environment variables
$required_vars = @(
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER", 
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_ROLE"
)

$missing_vars = @()
foreach ($var in $required_vars) {
    if (-not (Test-Path env:$var)) {
        $missing_vars += $var
    }
}

if ($missing_vars.Count -gt 0) {
    Write-Host "❌ Missing environment variables:" -ForegroundColor Red
    $missing_vars | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "`nSet them with:" -ForegroundColor Yellow
    Write-Host '$env:SNOWFLAKE_ACCOUNT = "your_account"' -ForegroundColor Cyan
    exit 1
}

Write-Host "✅ All environment variables configured" -ForegroundColor Green

# Check if SnowCLI is installed
try {
    snow --version | Out-Null
    Write-Host "✅ SnowCLI installed" -ForegroundColor Green
} catch {
    Write-Host "❌ SnowCLI not installed. Install with: pip install snowflake-cli-labs" -ForegroundColor Red
    exit 1
}

# Check if in correct directory
if (-not (Test-Path "streamlit_app.py")) {
    Write-Host "❌ Not in streamlit directory. Run from: src/project_spark/streamlit" -ForegroundColor Red
    exit 1
}

Write-Host "✅ In correct directory" -ForegroundColor Green

# Check all required files exist
$required_files = @(
    "streamlit_app.py",
    "environment.yml",
    "data_processor.py",
    "snowflake_operations.py",
    "snowpark_connection.py",
    "validators.py",
    "sql_templates.py",
    "constants.py"
)

$missing_files = @()
foreach ($file in $required_files) {
    if (-not (Test-Path $file)) {
        $missing_files += $file
    }
}

if ($missing_files.Count -gt 0) {
    Write-Host "❌ Missing required files:" -ForegroundColor Red
    $missing_files | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

Write-Host "✅ All required files present" -ForegroundColor Green
Write-Host "`n🎉 Deployment configuration validated successfully!" -ForegroundColor Green
Write-Host "`nTo deploy, configure GitHub secrets and push changes to main/dev branch" -ForegroundColor Cyan

