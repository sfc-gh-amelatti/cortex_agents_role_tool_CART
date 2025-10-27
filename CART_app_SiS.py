import streamlit as st
import pandas as pd
import json
import yaml
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.types import StringType
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="Snowflake Cortex Agent Permission Generator",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 0.375rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 0.375rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 0.375rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def get_snowflake_session() -> Optional[Session]:
    """Get the active Snowflake session."""
    try:
        session = get_active_session()
        return session
    except Exception as e:
        st.error(f"Error getting Snowflake session: {e}")
        return None

def parse_agent_tools_from_sql(session: Session, agent_database: str, agent_schema: str, agent_name: str) -> Dict:
    """Parse agent tools using SQL queries instead of REST API."""
    try:
        # Combined query that describes the agent and parses tools in one go
        combined_query = f"""
        WITH agent_describe AS (
            SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        ),
        parsed AS (
            SELECT 
                -- Get info from the 'tools' array
                tools_flat.VALUE:tool_spec:name::STRING AS TOOL_NAME,
                tools_flat.VALUE:tool_spec:type::STRING AS TOOL_TYPE,
                tools_flat.VALUE:tool_spec:description::STRING AS TOOL_DESCRIPTION,
                
                -- Path 1: Get DB/Schema from 'description' (your original logic)
                REGEXP_SUBSTR(
                    tools_flat.VALUE:tool_spec:description::STRING, 
                    'Database: (\\\\w+)', 1, 1, 'e', 1
                ) AS DB_FROM_DESC,
                REGEXP_SUBSTR(
                    tools_flat.VALUE:tool_spec:description::STRING, 
                    'Schema: (\\\\w+)', 1, 1, 'e', 1
                ) AS SCHEMA_FROM_DESC,

                -- Path 2: Get the full resource path from 'tool_resources'
                -- We check all known keys where a resource path might be
                COALESCE(
                    PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:identifier::STRING,
                    PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:semantic_view::STRING,
                    PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:search_service::STRING,
                    PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:name::STRING,
                    PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:semantic_model_file::STRING
                ) AS FULL_RESOURCE_PATH,
                
                -- Get procedure name with parameter types for generic tools
                PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:name::STRING AS PROCEDURE_NAME_WITH_TYPES,
                
                -- Get search service name for cortex_search tools (fallback when search_service is not available)
                PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:search_service::STRING AS SEARCH_SERVICE_NAME,
                
                -- Get semantic model file path for cortex_analyst_text_to_sql tools
                PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:semantic_model_file::STRING AS SEMANTIC_MODEL_FILE,
                
                -- Get execution environment info
                PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:execution_environment AS EXECUTION_ENV,
                
                -- Extract warehouse from execution_environment
                PARSE_JSON(desc_results."agent_spec"):tool_resources[TOOL_NAME]:execution_environment:warehouse::STRING AS TOOL_WH

            FROM 
                agent_describe AS desc_results,
                LATERAL FLATTEN(input => PARSE_JSON(desc_results."agent_spec"):tools) AS tools_flat
        )
        SELECT 
            TOOL_NAME,
            TOOL_TYPE,
            TOOL_DESCRIPTION,
            
            -- Final Columns: 
            -- If DB_FROM_DESC is null, use the value from FULL_RESOURCE_PATH
            COALESCE(
                DB_FROM_DESC, 
                SPLIT_PART(FULL_RESOURCE_PATH, '.', 1)
            ) AS DATABASE_NAME,
            
            -- If SCHEMA_FROM_DESC is null, use the value from FULL_RESOURCE_PATH
            COALESCE(
                SCHEMA_FROM_DESC, 
                SPLIT_PART(FULL_RESOURCE_PATH, '.', 2)
            ) AS SCHEMA_NAME,
            
            -- This extracts the 3rd part (the "MODEL" or object name)
            SPLIT_PART(FULL_RESOURCE_PATH, '.', 3) AS OBJECT_NAME,

            FULL_RESOURCE_PATH,
            PROCEDURE_NAME_WITH_TYPES,
            SEARCH_SERVICE_NAME,
            SEMANTIC_MODEL_FILE,
            EXECUTION_ENV,
            TOOL_WH
        FROM 
            parsed
        """
        
        # First execute DESCRIBE to populate RESULT_SCAN
        describe_query = f"DESCRIBE AGENT {agent_database}.{agent_schema}.{agent_name};"
        session.sql(describe_query).collect()
        
        # Then execute the combined parsing query
        results_df = session.sql(combined_query).collect()
        
        # Convert to pandas DataFrame for easier processing
        df = pd.DataFrame([row.asDict() for row in results_df])
        
        # Initialize collections
        semantic_views = set()
        semantic_model_files = set()
        semantic_model_stages = set()  # For stage permissions
        search_services = set()
        procedures = set()
        databases = set()
        schemas = set()
        tool_details = []
        tool_warehouses = {}
        
        # Process each tool
        for _, row in df.iterrows():
            tool_name = row['TOOL_NAME']
            tool_type = row['TOOL_TYPE']
            tool_description = row['TOOL_DESCRIPTION']
            database_name = row['DATABASE_NAME']
            schema_name = row['SCHEMA_NAME']
            object_name = row['OBJECT_NAME']
            full_resource_path = row['FULL_RESOURCE_PATH']
            procedure_name_with_types = row['PROCEDURE_NAME_WITH_TYPES']
            search_service_name = row['SEARCH_SERVICE_NAME']
            semantic_model_file = row['SEMANTIC_MODEL_FILE']
            execution_env = row['EXECUTION_ENV']
            tool_wh = row['TOOL_WH']
            
            tool_info = {
                "name": tool_name,
                "type": tool_type,
                "description": tool_description,
                "database": database_name,
                "schema": schema_name,
                "object": object_name,
                "full_path": full_resource_path,
                "procedure_name_with_types": procedure_name_with_types,
                "search_service_name": search_service_name,
                "semantic_model_file": semantic_model_file,
                "warehouse": tool_wh
            }
            
            # Extract warehouse information from TOOL_WH column
            if tool_wh and tool_wh.strip():
                tool_warehouses[tool_name] = tool_wh
                tool_info["warehouse"] = tool_wh
            
            # Categorize tools by type
            if tool_type == "cortex_analyst_text_to_sql":
                if semantic_model_file:
                    # Handle semantic model files stored in stages
                    semantic_model_files.add(semantic_model_file)
                    tool_info["semantic_model_file"] = semantic_model_file
                    # Extract database and schema from the semantic model file path
                    db, schema, stage = extract_stage_info_from_semantic_model_file(semantic_model_file)
                    if db and schema and stage:
                        # Add database and schema permissions for the stage location
                        databases.add(db)
                        schemas.add(f"{db}.{schema}")
                        # Add stage permission
                        semantic_model_stages.add(f"{db}.{schema}.{stage}")
                elif full_resource_path:
                    # Handle traditional semantic views
                    semantic_views.add(full_resource_path)
                    tool_info["semantic_view"] = full_resource_path
                    # Extract database and schema from the full_resource_path for semantic views
                    # Format is typically: DATABASE.SCHEMA.SEMANTIC_VIEW_NAME
                    path_parts = full_resource_path.split('.')
                    if len(path_parts) >= 2:
                        view_db = path_parts[0]
                        view_schema = path_parts[1]
                        databases.add(view_db)
                        schemas.add(f"{view_db}.{view_schema}")
                    else:
                        # Fallback to parsed values if path parsing fails
                        databases.add(database_name)
                        schemas.add(f"{database_name}.{schema_name}")
                    
            elif tool_type == "cortex_search":
                # Handle Cortex Search Services - use search_service_name if available, otherwise use full_resource_path
                search_service_path = search_service_name or full_resource_path
                if search_service_path:
                    search_services.add(search_service_path)
                    tool_info["search_service"] = search_service_path
                    # Extract database and schema from the search service path
                    # Format is typically: DATABASE.SCHEMA.SEARCH_SERVICE_NAME
                    path_parts = search_service_path.split('.')
                    if len(path_parts) >= 2:
                        search_db = path_parts[0]
                        search_schema = path_parts[1]
                        databases.add(search_db)
                        schemas.add(f"{search_db}.{search_schema}")
                    else:
                        # Fallback to parsed values if path parsing fails
                        databases.add(database_name)
                        schemas.add(f"{database_name}.{schema_name}")
                    
            elif tool_type == "generic":
                if full_resource_path:
                    # Extract database and schema from the full_resource_path for procedures
                    # Format is typically: DATABASE.SCHEMA.PROCEDURE_NAME
                    path_parts = full_resource_path.split('.')
                    if len(path_parts) >= 2:
                        proc_db = path_parts[0]
                        proc_schema = path_parts[1]
                    else:
                        # Fallback to parsed values if path parsing fails
                        proc_db = database_name
                        proc_schema = schema_name
                    
                    # Use procedure name with types if available, otherwise use full_resource_path
                    if procedure_name_with_types:
                        # Combine database.schema with procedure name and types
                        procedure_signature = f"{proc_db}.{proc_schema}.{procedure_name_with_types}"
                    else:
                        procedure_signature = full_resource_path
                    procedures.add(procedure_signature)
                    tool_info["procedure"] = procedure_signature
                    databases.add(proc_db)
                    schemas.add(f"{proc_db}.{proc_schema}")
            
            tool_details.append(tool_info)
        
        # Add agent's own database and schema
        databases.add(agent_database)
        schemas.add(f"{agent_database}.{agent_schema}")
        
        return {
            "semantic_views": list(semantic_views),
            "semantic_model_files": list(semantic_model_files),
            "semantic_model_stages": list(semantic_model_stages),
            "search_services": list(search_services),
            "procedures": list(procedures),
            "databases": list(databases),
            "schemas": list(schemas),
            "tool_details": tool_details,
            "tool_warehouses": tool_warehouses,
            "agent_name": agent_name,
            "agent_database": agent_database,
            "agent_schema": agent_schema,
            "tools_df": df
        }
        
    except Exception as e:
        st.error(f"Error parsing agent tools: {e}")
        return {
            "semantic_views": [],
            "semantic_model_files": [],
            "semantic_model_stages": [],
            "search_services": [],
            "procedures": [],
            "databases": [],
            "schemas": [],
            "tool_details": [],
            "tool_warehouses": {},
            "agent_name": agent_name,
            "agent_database": agent_database,
            "agent_schema": agent_schema,
            "tools_df": pd.DataFrame()
        }

def extract_stage_info_from_semantic_model_file(semantic_model_file: str) -> Tuple[str, str, str]:
    """
    Extract stage information from semantic model file path.
    
    Args:
        semantic_model_file: Path like @DATABASE.SCHEMA.STAGE_NAME/file.yaml
    
    Returns:
        Tuple of (database, schema, stage_name)
    """
    if not semantic_model_file.startswith('@'):
        return None, None, None
    
    stage_path = semantic_model_file[1:]  # Remove @ prefix
    path_parts = stage_path.split('/')
    
    if len(path_parts) >= 1:
        stage_identifier = path_parts[0]  # DATABASE.SCHEMA.STAGE_NAME
        stage_parts = stage_identifier.split('.')
        
        if len(stage_parts) >= 3:
            return stage_parts[0], stage_parts[1], stage_parts[2]
    
    return None, None, None

def read_yaml_from_stage_session(session: Session, semantic_model_file: str) -> Optional[Dict]:
    """
    Read YAML content from a stage using Snowflake session.
    
    Args:
        session: Active Snowflake session
        semantic_model_file: Path to semantic model file (e.g., @DATABASE.SCHEMA.STAGE/file.yaml)
    
    Returns:
        Parsed YAML content as dictionary, or None if failed
    """
    try:
        # Extract stage information
        database, schema, stage_name = extract_stage_info_from_semantic_model_file(semantic_model_file)
        
        if not all([database, schema, stage_name]):
            st.write(f"  ⚠️  Could not parse stage information from {semantic_model_file}")
            return None
        
        file_name = semantic_model_file.split('/')[-1]
        
        st.write(f"  📥 Reading file from stage: @{database}.{schema}.{stage_name}/{file_name}")
        
        # First, check if the file exists using LIST
        list_query = f"LIST @{database}.{schema}.{stage_name}/{file_name}"
        try:
            list_result = session.sql(list_query).collect()
            if not list_result:
                st.write(f"  ⚠️  File not found: {semantic_model_file}")
                return None
        except Exception as e:
            st.write(f"  ⚠️  Error listing file: {e}")
            return None
        
        # Try a simpler approach: Use COPY INTO with a regular table and ROW_NUMBER for ordering
        try:
            st.write(f"  📖 Reading file content using COPY INTO...")
            
            # Create a regular table (not temporary) to avoid the stored procedure limitation
            table_name = f"YAML_TEMP_{abs(hash(semantic_model_file)) % 10000}"
            
            # Create table with row number for ordering
            create_query = f"""
            CREATE OR REPLACE TABLE {table_name} (
                row_num INTEGER AUTOINCREMENT,
                line_content STRING
            )
            """
            session.sql(create_query).collect()
            
            # Copy file content
            copy_query = f"""
            COPY INTO {table_name} (line_content)
            FROM @{database}.{schema}.{stage_name}/{file_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = NONE FIELD_OPTIONALLY_ENCLOSED_BY = NONE)
            ON_ERROR = 'CONTINUE'
            """
            session.sql(copy_query).collect()
            
            # Read content using ROW_NUMBER() for ordering
            select_query = f"""
            SELECT LISTAGG(line_content, '\\n') WITHIN GROUP (ORDER BY row_num) as file_content
            FROM {table_name}
            WHERE line_content IS NOT NULL
            """
            
            result = session.sql(select_query).collect()
            
            if result and result[0]['FILE_CONTENT']:
                file_content = result[0]['FILE_CONTENT']
                st.write(f"  ✅ File content read successfully ({len(file_content)} characters)")
                
                # Parse YAML content
                st.write(f"  🔍 Parsing YAML content...")
                yaml_data = yaml.safe_load(file_content)
                st.write(f"  ✅ YAML file parsed successfully!")
                
                # Clean up
                session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
                
                return yaml_data
            else:
                st.write(f"  ⚠️  No content found for {semantic_model_file}")
                session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
                return None
                
        except Exception as e:
            st.write(f"  ❌ Error reading file content: {e}")
            
            # Final fallback: Try without ordering
            try:
                st.write(f"  🔄 Trying final fallback approach...")
                
                # Create table without autoincrement
                table_name = f"YAML_TEMP_{abs(hash(semantic_model_file)) % 10000}"
                
                create_query = f"""
                CREATE OR REPLACE TABLE {table_name} (
                    line_content STRING
                )
                """
                session.sql(create_query).collect()
                
                # Copy file content
                copy_query = f"""
                COPY INTO {table_name}
                FROM @{database}.{schema}.{stage_name}/{file_name}
                FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = NONE FIELD_OPTIONALLY_ENCLOSED_BY = NONE)
                ON_ERROR = 'CONTINUE'
                """
                session.sql(copy_query).collect()
                
                # Read content without ordering
                select_query = f"""
                SELECT LISTAGG(line_content, '\\n') as file_content
                FROM {table_name}
                WHERE line_content IS NOT NULL
                """
                
                result = session.sql(select_query).collect()
                
                if result and result[0]['FILE_CONTENT']:
                    file_content = result[0]['FILE_CONTENT']
                    st.write(f"  ✅ File content read successfully ({len(file_content)} characters)")
                    
                    # Parse YAML content
                    st.write(f"  🔍 Parsing YAML content...")
                    yaml_data = yaml.safe_load(file_content)
                    st.write(f"  ✅ YAML file parsed successfully!")
                    
                    # Clean up
                    session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
                    
                    return yaml_data
                else:
                    st.write(f"  ⚠️  No content found for {semantic_model_file}")
                    session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
                    return None
                    
            except Exception as e2:
                st.write(f"  ❌ Final fallback approach also failed: {e2}")
                return None
            
    except Exception as e:
        st.write(f"  ❌ Error reading YAML from stage: {e}")
        return None

def extract_table_permissions_from_yaml(yaml_content: Dict) -> Tuple[List[Tuple[str, str, str]], List[str], str]:
    """
    Extract table permissions and Cortex Search Services from parsed YAML content and identify the format type.
    Enhanced to handle different YAML formats for semantic models.
    
    Args:
        yaml_content: Parsed YAML content from semantic view or semantic model file
    
    Returns:
        Tuple of (table_permissions_list, cortex_search_services_list, format_type)
    """
    table_permissions = []
    cortex_search_services = []
    format_type = "unknown"
    
    if not yaml_content:
        return table_permissions, cortex_search_services, format_type
    
    # Method 1: Check for semantic_model first (more specific)
    if "semantic_model" in yaml_content:
        format_type = "semantic model"
        semantic_model = yaml_content["semantic_model"]
        
        if "tables" in semantic_model:
            for table in semantic_model["tables"]:
                if isinstance(table, dict):
                    database = table.get("database") or table.get("db")
                    schema = table.get("schema") or table.get("schema_name")
                    table_name = table.get("table") or table.get("table_name") or table.get("name")
                    
                    if database and schema and table_name:
                        table_permissions.append((database, schema, table_name))
    
    # Method 2: Standard semantic view format (fallback)
    elif "tables" in yaml_content:
        format_type = "semantic view"
        for table in yaml_content["tables"]:
            if "base_table" in table:
                base_table = table["base_table"]
                database = base_table.get("database")
                schema = base_table.get("schema")
                table_name = base_table.get("table")
                
                if database and schema and table_name:
                    table_permissions.append((database, schema, table_name))
    
    # Extract Cortex Search Services from YAML content
    def find_cortex_search_services(obj, path=""):
        """Recursively find Cortex Search Service references in YAML structure"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key.lower() == 'cortex_search_service' and isinstance(value, dict):
                    database = value.get("database") or value.get("db")
                    schema = value.get("schema") or value.get("schema_name")
                    service = value.get("service") or value.get("service_name") or value.get("name")
                    
                    if database and schema and service:
                        service_path = f"{database}.{schema}.{service}"
                        cortex_search_services.append(service_path)
                elif isinstance(value, (dict, list)):
                    find_cortex_search_services(value, f"{path}.{key}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, (dict, list)):
                    find_cortex_search_services(item, f"{path}[{i}]")
    
    # Run the recursive search for Cortex Search Services
    find_cortex_search_services(yaml_content)
    
    # Method 3: Recursive search for table references in any nested structure
    def find_table_references(obj, path=""):
        """Recursively find table references in YAML structure"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key.lower() in ['table', 'base_table', 'source_table'] and isinstance(value, dict):
                    database = value.get("database") or value.get("db")
                    schema = value.get("schema") or value.get("schema_name")
                    table_name = value.get("table") or value.get("table_name") or value.get("name")
                    
                    if database and schema and table_name:
                        table_permissions.append((database, schema, table_name))
                elif isinstance(value, (dict, list)):
                    find_table_references(value, f"{path}.{key}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, (dict, list)):
                    find_table_references(item, f"{path}[{i}]")
    
    # Run the recursive search
    find_table_references(yaml_content)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_permissions = []
    for perm in table_permissions:
        if perm not in seen:
            seen.add(perm)
            unique_permissions.append(perm)
    
    # Remove duplicates from Cortex Search Services
    seen_services = set()
    unique_services = []
    for service in cortex_search_services:
        if service not in seen_services:
            seen_services.add(service)
            unique_services.append(service)
    
    return unique_permissions, unique_services, format_type

def execute_semantic_model_file_queries(session: Session, semantic_model_files: List[str]) -> Tuple[Dict[str, List[Tuple[str, str, str]]], Dict[str, List[str]]]:
    """Execute semantic model file queries and extract table permissions and Cortex Search Services."""
    table_results = {}
    search_service_results = {}
    
    for semantic_model_file in semantic_model_files:
        try:
            st.write(f"Processing semantic model file: {semantic_model_file}")
            
            # Read YAML content from stage using session
            yaml_content = read_yaml_from_stage_session(session, semantic_model_file)
            
            if yaml_content:
                # Extract table permissions, Cortex Search Services, and format type
                table_permissions, cortex_search_services, format_type = extract_table_permissions_from_yaml(yaml_content)
                table_results[semantic_model_file] = table_permissions
                search_service_results[semantic_model_file] = cortex_search_services
                
                st.write(f"  Found {len(table_permissions)} tables: {[f'{db}.{schema}.{table}' for db, schema, table in table_permissions]}")
                if cortex_search_services:
                    st.write(f"  Found {len(cortex_search_services)} Cortex Search Services: {cortex_search_services}")
            else:
                st.write(f"  No YAML content found for {semantic_model_file}")
                table_results[semantic_model_file] = []
                search_service_results[semantic_model_file] = []
                
        except Exception as e:
            st.error(f"  Error processing {semantic_model_file}: {e}")
            table_results[semantic_model_file] = []
            search_service_results[semantic_model_file] = []
    
    return table_results, search_service_results

def execute_semantic_view_queries(session: Session, semantic_views: List[str]) -> Tuple[Dict[str, List[Tuple[str, str, str]]], Dict[str, List[str]]]:
    """Execute semantic view queries and extract table permissions and Cortex Search Services."""
    table_results = {}
    search_service_results = {}
    
    for semantic_view in semantic_views:
        try:
            # Execute the query
            query = f"SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW('{semantic_view}') as yaml_content"
            result = session.sql(query).collect()
            
            if result and result[0]['YAML_CONTENT']:
                # Parse YAML content
                yaml_content = yaml.safe_load(result[0]['YAML_CONTENT'])
                
                # Extract table permissions, Cortex Search Services, and format type
                table_permissions, cortex_search_services, format_type = extract_table_permissions_from_yaml(yaml_content)
                table_results[semantic_view] = table_permissions
                search_service_results[semantic_view] = cortex_search_services
                
                # Use appropriate processing message based on format type
                if format_type == "semantic model":
                    st.write(f"Processing semantic model: {semantic_view}")
                else:
                    st.write(f"Processing semantic view: {semantic_view}")
                
                st.write(f"  Found {len(table_permissions)} tables: {[f'{db}.{schema}.{table}' for db, schema, table in table_permissions]}")
                if cortex_search_services:
                    st.write(f"  Found {len(cortex_search_services)} Cortex Search Services: {cortex_search_services}")
                
            else:
                st.write(f"Processing semantic view: {semantic_view}")
                st.write(f"  No YAML content found for {semantic_view}")
                table_results[semantic_view] = []
                search_service_results[semantic_view] = []
                
        except Exception as e:
            st.error(f"  Error processing {semantic_view}: {e}")
            table_results[semantic_view] = []
            search_service_results[semantic_view] = []
    
    return table_results, search_service_results

def generate_comprehensive_permission_script(
    parsed_tools: Dict,
    table_permissions_results: Dict[str, List[Tuple[str, str, str]]],
    yaml_cortex_search_services: Set[str],
    warehouse_name: str = "COMPUTE_WH"
) -> str:
    """Generate comprehensive SQL permission script."""
    agent_name = parsed_tools["agent_name"]
    agent_database = parsed_tools["agent_database"]
    agent_schema = parsed_tools["agent_schema"]
    fully_qualified_agent = f"{agent_database}.{agent_schema}.{agent_name}"
    
    # Collect all unique table permissions
    all_table_permissions = set()
    for tables in table_permissions_results.values():
        for db, schema, table in tables:
            all_table_permissions.add(f"{db}.{schema}.{table}")
    
    # Generate database and schema USAGE grants
    all_db_grants = set(parsed_tools["databases"])
    all_schema_grants = set(parsed_tools["schemas"])
    
    # CRITICAL: Add database and schema grants for tables discovered in semantic view YAML
    # that are not already covered by the agent tool specifications
    table_db_grants = set()
    table_schema_grants = set()
    
    for tables in table_permissions_results.values():
        for db, schema, table in tables:
            table_db_grants.add(db)
            table_schema_grants.add(f"{db}.{schema}")
    
    # Combine all database and schema grants
    all_db_grants = all_db_grants.union(table_db_grants)
    all_schema_grants = all_schema_grants.union(table_schema_grants)
    
    # Generate permission grants
    db_grants = "\n".join([f"GRANT USAGE ON DATABASE {db} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);" 
                          for db in sorted(all_db_grants)])
    
    schema_grants = "\n".join([f"GRANT USAGE ON SCHEMA {schema} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);" 
                              for schema in sorted(all_schema_grants)])
    
    view_grants = "\n".join([f"GRANT SELECT ON VIEW {view} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);" 
                            for view in sorted(parsed_tools["semantic_views"])])
    
    table_grants = "\n".join([f"GRANT SELECT ON TABLE {table} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);" 
                            for table in sorted(all_table_permissions)])
    
    # Combine tool-specified and YAML-extracted Cortex Search Services
    all_search_services = set(parsed_tools["search_services"]).union(yaml_cortex_search_services)
    
    search_grants = "\n".join([f"GRANT USAGE ON CORTEX SEARCH SERVICE {service} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);" 
                              for service in sorted(all_search_services)])
    
    procedure_grants = "\n".join([f"GRANT USAGE ON PROCEDURE {procedure} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);" 
                                for procedure in sorted(parsed_tools.get("procedures", []))])
    
    # Generate stage grants for semantic model files
    stage_grants = "\n".join([f"GRANT READ ON STAGE {stage} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);" 
                            for stage in sorted(parsed_tools.get("semantic_model_stages", []))])
    
    # Generate tool-specific warehouse grants
    tool_warehouse_grants = ""
    if parsed_tools.get("tool_warehouses"):
        tool_warehouse_grants = "\n".join([
            f"GRANT USAGE ON WAREHOUSE IDENTIFIER('{warehouse}') TO ROLE IDENTIFIER($AGENT_ROLE_NAME); -- Required for tool: {tool_name}"
            for tool_name, warehouse in parsed_tools["tool_warehouses"].items()
        ])
        if tool_warehouse_grants:
            tool_warehouse_grants = f"\n-- Tool-specific warehouse permissions\n{tool_warehouse_grants}"
    
    # Assemble the complete script
    script = f"""-- =========================================================================================
-- AUTO-GENERATED LEAST-PRIVILEGE SCRIPT FOR AGENT: {fully_qualified_agent}
-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Generated by: Snowflake Cortex Agent Permission Generator
-- =========================================================================================

-- IMPORTANT: Review and adjust the placeholder variables below for your environment.
SET AGENT_ROLE_NAME = '{agent_name}_USER_ROLE';
SET WAREHOUSE_NAME = '{warehouse_name}';

-- Create a dedicated role for the agent's permissions.
USE ROLE SECURITYADMIN; -- Or your own privileged role to assign permissions
CREATE ROLE IF NOT EXISTS IDENTIFIER($AGENT_ROLE_NAME);
GRANT ROLE IDENTIFIER($AGENT_ROLE_NAME) TO ROLE SYSADMIN; -- Optional: Allows SYSADMIN to manage the role.

-- Grant core permission to use the agent object itself.
GRANT USAGE ON AGENT {fully_qualified_agent} TO ROLE IDENTIFIER($AGENT_ROLE_NAME);

-- Grant permissions on the underlying database objects required by the agent's tools.
-- NOTE: These permissions are derived from the agent's tool specification and semantic view YAML definitions.

-- Database and Schema USAGE grants (including agent location, tool-specific locations, and tables from semantic views)
{db_grants}
{schema_grants}

-- Permissions for 'cortex_analyst_text_to_sql' tools
-- Semantic view permissions
{view_grants}

-- Base table permissions (from semantic view YAML)
{table_grants}

-- Permissions for 'cortex_search' tools
{search_grants}

-- Permissions for 'generic' tools (procedures)
{procedure_grants}

-- Permissions for semantic model files (stages)
{stage_grants}

{tool_warehouse_grants}

-- Grant warehouse usage to the role for the user's session.
GRANT USAGE ON WAREHOUSE IDENTIFIER($WAREHOUSE_NAME) TO ROLE IDENTIFIER($AGENT_ROLE_NAME);

-- =========================================================================================
SELECT 'Setup complete for role ' || $AGENT_ROLE_NAME AS "Status";
-- =========================================================================================
"""
    
    return script

def main():
    # Header
    st.markdown('<h1 class="main-header">🔐 Snowflake Cortex Agent Permission Generator</h1>', unsafe_allow_html=True)
    
    st.markdown("""
    This application automates the process of generating least-privilege SQL scripts for Snowflake Cortex Agents by:
    
    1. **SQL Query Analysis**: Using SQL queries to DESCRIBE agent objects and extract relevant tool information
    2. **Semantic View Processing**: Extracting table permissions from semantic view YAML definitions
    3. **Permission Script Generation**: Generating comprehensive SQL scripts for least-privilege access
    """)
    
    # Sidebar for information
    st.sidebar.header("ℹ️ About")
    st.sidebar.info("""
    This app uses your active Snowflake session to analyze Cortex Agents and generate permission scripts.
    
    **Requirements:**
    - Active Snowflake session
    - Permissions to DESCRIBE agents
    - Access to semantic views
    """)
    
    # Test session button
    if st.sidebar.button("Test Session"):
        session = get_snowflake_session()
        if session:
            st.sidebar.success("✅ Session active!")
        else:
            st.sidebar.error("❌ No active session!")
    
    # Main content area
    st.markdown('<div class="section-header">Agent Configuration</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        agent_database = st.text_input("Agent Database", value="SNOWFLAKE_INTELLIGENCE")
    with col2:
        agent_schema = st.text_input("Agent Schema", value="AGENTS")
    with col3:
        agent_name = st.text_input("Agent Name", value="SUBCONTRACTOR_AI")
    
    # Process button
    if st.button("🚀 Generate Permission Script", type="primary"):
        if not agent_database or not agent_schema or not agent_name:
            st.error("Please fill in all agent fields")
            return
        
        # Get active session
        session = get_snowflake_session()
        if not session:
            st.error("No active Snowflake session found. Please ensure you're running this in Snowflake.")
            return
        
        # Parse agent tools
        with st.spinner("Parsing agent tools..."):
            parsed_tools = parse_agent_tools_from_sql(session, agent_database, agent_schema, agent_name)
        
        if parsed_tools["tools_df"].empty:
            st.error("No tools found in agent specification")
            return
        
        # Display parsed tools
        st.markdown('<div class="section-header">📋 Parsed Tool Information</div>', unsafe_allow_html=True)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total Tools", len(parsed_tools["tool_details"]))
        with col2:
            st.metric("Semantic Views", len(parsed_tools["semantic_views"]))
        with col3:
            st.metric("Semantic Model Files", len(parsed_tools["semantic_model_files"]))
        with col4:
            st.metric("Semantic Model Stages", len(parsed_tools["semantic_model_stages"]))
        with col5:
            st.metric("Search Services", len(parsed_tools["search_services"]))
        
        # Display tools table
        st.subheader("Tools Overview")
        st.dataframe(parsed_tools["tools_df"], use_container_width=True)
        
        # Process semantic views and semantic model files
        table_permissions_results = {}
        yaml_cortex_search_services = set()  # Collect Cortex Search Services from YAML content
        
        if parsed_tools["semantic_views"]:
            with st.spinner("Processing semantic views..."):
                semantic_view_table_results, semantic_view_search_results = execute_semantic_view_queries(session, parsed_tools["semantic_views"])
                table_permissions_results.update(semantic_view_table_results)
                # Collect Cortex Search Services from semantic views
                for search_services in semantic_view_search_results.values():
                    yaml_cortex_search_services.update(search_services)
        
        if parsed_tools["semantic_model_files"]:
            with st.spinner("Processing semantic model files..."):
                semantic_model_table_results, semantic_model_search_results = execute_semantic_model_file_queries(session, parsed_tools["semantic_model_files"])
                table_permissions_results.update(semantic_model_table_results)
                # Collect Cortex Search Services from semantic model files
                for search_services in semantic_model_search_results.values():
                    yaml_cortex_search_services.update(search_services)
        
        # Generate permission script
        with st.spinner("Generating permission script..."):
            permission_script = generate_comprehensive_permission_script(
                parsed_tools=parsed_tools,
                table_permissions_results=table_permissions_results,
                yaml_cortex_search_services=yaml_cortex_search_services,
                warehouse_name="COMPUTE_WH"
            )
        
        # Display results
        st.markdown('<div class="section-header">📜 Generated Permission Script</div>', unsafe_allow_html=True)
        
        # Calculate final database and schema counts including tables from semantic views
        final_db_count = len(set(parsed_tools['databases']).union(
            {db for tables in table_permissions_results.values() for db, schema, table in tables}
        ))
        final_schema_count = len(set(parsed_tools['schemas']).union(
            {f"{db}.{schema}" for tables in table_permissions_results.values() for db, schema, table in tables}
        ))
        
        # Summary
        st.info(f"""
        **Agent**: {parsed_tools['agent_name']}  
        **Location**: {parsed_tools['agent_database']}.{parsed_tools['agent_schema']}  
        **Databases**: {final_db_count} (including tables from semantic views)  
        **Schemas**: {final_schema_count} (including tables from semantic views)  
        **Tables**: {sum(len(tables) for tables in table_permissions_results.values())}
        """)
        
        # Script display
        st.code(permission_script, language="sql")
        
        # Download button
        st.download_button(
            label="📥 Download SQL Script",
            data=permission_script,
            file_name=f"{agent_name}_permissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )
        
        # Store in session state for potential reuse
        st.session_state.last_permission_script = permission_script
        st.session_state.last_parsed_tools = parsed_tools

if __name__ == "__main__":
    main()
