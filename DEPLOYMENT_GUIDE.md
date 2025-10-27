# Snowflake Streamlit Deployment Guide

## Quick Setup

1. **Upload the app file to Snowflake**:
   ```sql
   PUT file://streamlit_app.py @your_stage/;
   ```

2. **Create the Streamlit app**:
   ```sql
   CREATE STREAMLIT cortex_agent_permissions
   FROM LOCATION '@your_stage/streamlit_app.py'
   MAIN_FILE = 'streamlit_app.py';
   ```

3. **Grant permissions**:
   ```sql
   GRANT USAGE ON STREAMLIT cortex_agent_permissions TO ROLE your_role;
   ```

## Usage

1. Navigate to your Streamlit app in Snowflake
2. Enter agent details:
   - Database: `SNOWFLAKE_INTELLIGENCE`
   - Schema: `AGENTS`
   - Agent: `SUBCONTRACTOR_AI`
3. Click "Generate Permission Script"
4. Download the SQL script

## Key Features

- ✅ Uses `get_active_session()` - no connection configuration needed
- ✅ SQL-based agent analysis using your provided queries
- ✅ Semantic view YAML processing
- ✅ Comprehensive permission script generation
- ✅ Interactive UI with real-time feedback

## Required Permissions

- `USAGE` on databases/schemas containing agents
- `DESCRIBE` permission on agent objects
- `USAGE` on semantic views
- `USAGE` on warehouses for execution
