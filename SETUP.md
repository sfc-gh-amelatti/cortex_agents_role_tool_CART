# Automated Agent Permissions - Setup Guide

## Overview
This notebook automates the process of generating least-privilege SQL scripts for Snowflake Cortex Agents by parsing agent specifications and extracting required permissions from semantic views and semantic model files.

## Prerequisites

### 1. Python Environment
- Python 3.8 or higher
- pip package manager

### 2. Snowflake Access
- Snowflake account with appropriate permissions
- Ability to query agent objects and semantic views
- Access to stages containing semantic model files

### 3. Environment Variables
Create a `.env` file in the project root with the following variables:

```bash
# Snowflake Connection Details
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_USER_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse

#For REST API calls
SNOWFLAKE_BEARER_TOKEN=your_bearer_token
```

## Installation

### 1. Install Required Packages
```bash
pip install -r requirements.txt
```

### 2. Verify Installation
Run the first cell of the notebook to verify all libraries are imported successfully.

## Usage

### 1. Configure Environment
- Set up your `.env` file with Snowflake credentials
- Ensure you have appropriate permissions to access agent objects and stages

### 2. Run the Notebook
- Execute cells in order
- The notebook will automatically:
  - Parse agent specifications
  - Extract semantic view information
  - Download and parse semantic model files from stages
  - Generate comprehensive permission scripts

### 3. Generated Output
The notebook produces SQL scripts that include:
- Database and schema USAGE permissions
- Semantic view SELECT permissions
- Stage READ permissions for semantic model files
- Table SELECT permissions extracted from YAML files
- Tool-specific warehouse permissions

## Key Features

- **Automated YAML Processing**: Uses `snowflake.connector` to download and parse YAML files from stages
- **Comprehensive Permission Extraction**: Handles multiple YAML formats and table reference patterns
- **Stage Permission Generation**: Automatically generates READ permissions for stages containing semantic model files
- **Error Handling**: Robust error handling with graceful fallbacks
- **Clean Integration**: Works alongside existing semantic view processing

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify Snowflake credentials in `.env` file
   - Check network connectivity
   - Ensure warehouse is running

2. **Permission Errors**
   - Verify you have USAGE on required databases/schemas
   - Check stage READ permissions
   - Ensure agent object access permissions

3. **YAML Parsing Errors**
   - Check YAML file format and syntax
   - Verify file exists in the specified stage
   - Review stage path format: `@DATABASE.SCHEMA.STAGE/file.yaml`

### Support
For issues or questions, refer to the Snowflake documentation or contact your Snowflake administrator.
