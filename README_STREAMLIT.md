# Snowflake Cortex Agent Permission Generator - Streamlit App

This Streamlit application runs **inside Snowflake** and automates the process of generating least-privilege SQL scripts for Snowflake Cortex Agents by analyzing agent specifications and extracting required permissions.

## Features

- **Native Snowflake Integration**: Uses `get_active_session()` for seamless Snowflake integration
- **SQL-Based Analysis**: Uses Snowflake SQL queries instead of REST API calls
- **Agent Tool Parsing**: Extracts tool information from agent specifications
- **Semantic View Processing**: Analyzes semantic view YAML definitions to identify required table permissions
- **Comprehensive Permission Scripts**: Generates complete SQL scripts for least-privilege access
- **Interactive UI**: User-friendly interface for configuration and execution

## Prerequisites

- Snowflake account with Streamlit enabled
- Appropriate permissions to DESCRIBE agents and access semantic views
- Python 3.8 or higher (handled by Snowflake)

## Installation

1. **Upload to Snowflake**:
   - Upload `streamlit_app.py` to your Snowflake environment
   - Ensure you have the required Python packages available

2. **Create Streamlit App in Snowflake**:
   ```sql
   CREATE STREAMLIT app_name
   FROM LOCATION '@your_stage/streamlit_app.py'
   MAIN_FILE = 'streamlit_app.py';
   ```

## Usage

1. **Access the app**:
   - Navigate to your Streamlit app in Snowflake
   - The app will automatically use your active Snowflake session

2. **Enter agent details**:
   - Database name (e.g., `SNOWFLAKE_INTELLIGENCE`)
   - Schema name (e.g., `AGENTS`)
   - Agent name (e.g., `SUBCONTRACTOR_AI`)

3. **Generate permission script**:
   - Click "Generate Permission Script"
   - Review the parsed tool information
   - Download the generated SQL script

## How It Works

### 1. Agent Analysis
The app uses SQL queries to analyze agent specifications:
```sql
DESCRIBE AGENT database.schema.agent_name;
```

### 2. Tool Parsing
Extracts tool information using a complex SQL query that:
- Parses the `agent_spec` JSON
- Extracts tool names, types, and descriptions
- Identifies resource paths from `tool_resources`
- Categorizes tools by type (cortex_analyst_text_to_sql, cortex_search, generic)

### 3. Semantic View Processing
For `cortex_analyst_text_to_sql` tools:
- Executes `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW()` function
- Parses YAML content to identify base tables
- Extracts database, schema, and table information

### 4. Permission Generation
Generates comprehensive SQL scripts including:
- Database and schema USAGE grants
- Semantic view SELECT permissions
- Base table SELECT permissions
- Search service USAGE permissions
- Procedure USAGE permissions
- Warehouse USAGE permissions

## Generated Script Structure

The generated SQL script includes:

```sql
-- Role creation
CREATE ROLE IF NOT EXISTS IDENTIFIER($AGENT_ROLE_NAME);

-- Agent usage permission
GRANT USAGE ON AGENT database.schema.agent_name TO ROLE IDENTIFIER($AGENT_ROLE_NAME);

-- Database and schema permissions
GRANT USAGE ON DATABASE database_name TO ROLE IDENTIFIER($AGENT_ROLE_NAME);
GRANT USAGE ON SCHEMA database.schema TO ROLE IDENTIFIER($AGENT_ROLE_NAME);

-- Tool-specific permissions
GRANT SELECT ON VIEW semantic_view TO ROLE IDENTIFIER($AGENT_ROLE_NAME);
GRANT SELECT ON TABLE database.schema.table TO ROLE IDENTIFIER($AGENT_ROLE_NAME);
GRANT USAGE ON CORTEX SEARCH SERVICE search_service TO ROLE IDENTIFIER($AGENT_ROLE_NAME);
GRANT USAGE ON PROCEDURE database.schema.procedure TO ROLE IDENTIFIER($AGENT_ROLE_NAME);
```

## Configuration Options

### Session Management
- **Automatic**: Uses `get_active_session()` for seamless integration
- **No Configuration Required**: Works with your current Snowflake session

### Customization
- Modify warehouse names in the generated script
- Adjust role naming conventions
- Add additional permission types as needed

## Troubleshooting

### Common Issues

1. **No Active Session**:
   - Ensure you're running the app in Snowflake Streamlit
   - Check that you have an active Snowflake session
   - Verify proper permissions

2. **No Tools Found**:
   - Verify agent exists in specified database/schema
   - Check agent specification format
   - Ensure proper permissions to DESCRIBE agent

3. **Semantic View Errors**:
   - Verify semantic view exists
   - Check permissions to read semantic view YAML
   - Ensure semantic view is properly configured

### Required Permissions

The application requires the following Snowflake permissions:
- `USAGE` on databases and schemas containing agents
- `DESCRIBE` permission on agent objects
- `USAGE` on semantic views
- `USAGE` on warehouses for execution

## Security Considerations

- Generated scripts use least-privilege principles
- Review all generated permissions before execution
- Consider using dedicated service accounts
- Implement proper role hierarchy

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue in the repository
- Check existing documentation
- Review Snowflake Cortex Agent documentation
