"""
DuckDB helper functions for InLaw data expectations.

This module provides utilities to connect to DuckDB files in the saw_cache directory
and run InLaw validation tests against them using SQL-on-FHIR ViewDefinitions.
"""
import duckdb
import json
from pathlib import Path
from typing import Optional
import pandas as pd
import great_expectations as gx



class DuckDBHelper:
    """Helper class for connecting to DuckDB files for InLaw tests."""
    
    @staticmethod
    def get_connection(*, resource_type: str, cache_dir: str = "../saw_cache") -> duckdb.DuckDBPyConnection:
        """
        Create a DuckDB connection to a specific resource type database.
        
        Args:
            resource_type: The FHIR resource type (e.g., 'practitioner', 'organization')
            cache_dir: Directory containing the DuckDB files
            
        Returns:
            DuckDB connection object
            
        Raises:
            FileNotFoundError: If the DuckDB file doesn't exist
        """
        cache_path = Path(cache_dir)
        db_file = cache_path / f"{resource_type}.duckdb"
        
        if not db_file.exists():
            raise FileNotFoundError(f"DuckDB file not found: {db_file}")
        
        # Connect to the DuckDB file (read_only=False to allow view creation)
        conn = duckdb.connect(str(db_file), read_only=False)
        return conn
    
    @staticmethod
    def _fhirpath_to_json_extract(fhir_path: str) -> str:
        """
        Convert simple FHIRPath expressions to DuckDB JSON extraction SQL.
        This is a simplified converter for common patterns in ViewDefinitions.
        
        Args:
            fhir_path: FHIRPath expression
            
        Returns:
            DuckDB JSON extraction expression
        """
        # Handle simple paths like "id", "active", "gender"
        if '.' not in fhir_path and '[' not in fhir_path and 'where' not in fhir_path:
            return f"json_extract_string(resource, '$.{fhir_path}')"
        
        # Handle identifier.where(system='..').value.first()
        if 'identifier.where' in fhir_path and 'us-npi' in fhir_path:
            # Extract NPI from identifier array
            return """(
                SELECT json_extract_string(value, '$.value')
                FROM json_each(json_extract(resource, '$.identifier'))
                WHERE json_extract_string(value, '$.system') = 'http://hl7.org/fhir/sid/us-npi'
                LIMIT 1
            )"""
        
        # Handle name[0].given[0] - first given name
        if fhir_path == 'name[0].given[0]':
            return "json_extract_string(resource, '$.name[0].given[0]')"
        
        # Handle name[0].given[1] - middle name
        if fhir_path == 'name[0].given[1]':
            return "json_extract_string(resource, '$.name[0].given[1]')"
        
        # Handle name[0].family - last name
        if fhir_path == 'name[0].family':
            return "json_extract_string(resource, '$.name[0].family')"
        
        # Handle name[0].prefix[0]
        if fhir_path == 'name[0].prefix[0]':
            return "json_extract_string(resource, '$.name[0].prefix[0]')"
        
        # Handle name[0].suffix[0]
        if fhir_path == 'name[0].suffix[0]':
            return "json_extract_string(resource, '$.name[0].suffix[0]')"
        
        # Handle qualification array - count
        if fhir_path == 'qualification':
            return "json_array_length(json_extract(resource, '$.qualification'))"
        
        # Handle communication array - count
        if fhir_path == 'communication':
            return "json_array_length(json_extract(resource, '$.communication'))"
        
        # Default: try basic json_extract_string
        return f"json_extract_string(resource, '$.{fhir_path}')"
    
    @staticmethod
    def create_view_from_viewdef(*, conn: duckdb.DuckDBPyConnection, viewdef_path: str, view_name: str = None) -> str:
        """
        Create a DuckDB view from a SQL-on-FHIR ViewDefinition JSON file.
        
        Args:
            conn: DuckDB connection
            viewdef_path: Path to ViewDefinition JSON file
            view_name: Optional custom view name (defaults to viewdef name)
            
        Returns:
            Name of the created view
        """
        # Load ViewDefinition
        with open(viewdef_path, 'r') as f:
            viewdef = json.load(f)
        
        # Get view name
        if view_name is None:
            view_name = viewdef.get('name', 'fhir_view')
        
        # Build SELECT clause from ViewDefinition columns
        select_parts = []
        for col in viewdef['select'][0]['column']:
            col_name = col['name']
            fhir_path = col['path']
            sql_expr = DuckDBHelper._fhirpath_to_json_extract(fhir_path)
            select_parts.append(f"    {sql_expr} AS {col_name}")
        
        # Create view SQL
        select_clause = ',\n'.join(select_parts)
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT
{select_clause}
        FROM fhir_resources
        """
        
        # Execute
        conn.execute(create_view_sql)
        return view_name

    
    @staticmethod
    def get_table_name(*, resource_type: str) -> str:
        """
        Get the expected table name for a resource type.
        
        DuckDB tables are typically named as resource_type_resource_type
        (e.g., practitioner_practitioner, organization_organization)
        
        Args:
            resource_type: The FHIR resource type
            
        Returns:
            Table name string
        """
        return f"{resource_type}_{resource_type}"
    
    @staticmethod
    def execute_query(*, resource_type: str, sql: str, cache_dir: str = "../saw_cache"):
        """
        Execute a SQL query against a DuckDB resource database.
        
        Args:
            resource_type: The FHIR resource type
            sql: SQL query to execute
            cache_dir: Directory containing the DuckDB files
            
        Returns:
            Query results as a list of tuples
        """
        conn = DuckDBHelper.get_connection(resource_type=resource_type, cache_dir=cache_dir)
        try:
            result = conn.execute(sql).fetchall()
            return result
        finally:
            conn.close()
    
    @staticmethod
    def query_to_dataframe(*, resource_type: str, sql: str, cache_dir: str = "../saw_cache"):
        """
        Execute a SQL query and return results as a pandas DataFrame.
        
        Args:
            resource_type: The FHIR resource type
            sql: SQL query to execute
            cache_dir: Directory containing the DuckDB files
            
        Returns:
            Pandas DataFrame with query results
        """
        conn = DuckDBHelper.get_connection(resource_type=resource_type, cache_dir=cache_dir)
        try:
            result = conn.execute(sql).df()
            return result
        finally:
            conn.close()
    
    @staticmethod
    def sql_to_gx_df(*, resource_type: str, sql: str, cache_dir: str = "../saw_cache"):
        """
        Execute SQL against DuckDB and return Great Expectations Validator.
        This is the InLaw-compatible method for DuckDB validation.
        
        Args:
            resource_type: The FHIR resource type
            sql: SQL query to execute
            cache_dir: Directory containing the DuckDB files
            
        Returns:
            Great Expectations Validator for validation
        """
        # Get pandas DataFrame from DuckDB query
        pandas_df = DuckDBHelper.query_to_dataframe(
            resource_type=resource_type,
            sql=sql,
            cache_dir=cache_dir
        )
        
        # Convert to Great Expectations Validator
        context = gx.get_context()
        datasource = context.sources.add_pandas("pandas_datasource")
        data_asset = datasource.add_dataframe_asset("dataframe_asset")
        batch_request = data_asset.build_batch_request(dataframe=pandas_df)
        gx_validator = context.get_validator(batch_request=batch_request)
        
        return gx_validator

