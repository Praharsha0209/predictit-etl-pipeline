"""
Snowflake Loader
Handles loading data into Snowflake data warehouse
"""

import snowflake.connector
import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Load and transform data in Snowflake"""
    
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str,
        role: str = 'ACCOUNTADMIN'
    ):
        """
        Initialize Snowflake loader
        
        Args:
            account: Snowflake account identifier
            user: Username
            password: Password
            warehouse: Warehouse name
            database: Database name
            schema: Schema name
            role: Role name
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for Snowflake connection
        
        Yields:
            Snowflake connection object
        """
        conn = None
        try:
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role
            )
            logger.info(f"Connected to Snowflake: {self.database}.{self.schema}")
            yield conn
            
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {str(e)}")
            raise
            
        finally:
            if conn:
                conn.close()
                logger.info("Snowflake connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Execute a SQL query
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query results
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                results = cursor.fetchall()
                logger.info(f"Query executed successfully, returned {len(results)} rows")
                return results
                
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                logger.error(f"Query: {query}")
                raise
                
            finally:
                cursor.close()
    
    def execute_many(self, query: str, data: List[tuple]) -> int:
        """
        Execute a query multiple times with different parameters
        
        Args:
            query: SQL query string
            data: List of parameter tuples
            
        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(query, data)
                row_count = cursor.rowcount
                logger.info(f"Executed query {len(data)} times, affected {row_count} rows")
                return row_count
                
            except Exception as e:
                logger.error(f"Error executing batch query: {str(e)}")
                raise
                
            finally:
                cursor.close()
    
    def create_stage(
        self,
        stage_name: str,
        s3_bucket: str,
        s3_path: str = '',
        aws_key_id: Optional[str] = None,
        aws_secret_key: Optional[str] = None,
        file_format: str = 'JSON'
    ) -> bool:
        """
        Create an external stage pointing to S3
        
        Args:
            stage_name: Name for the stage
            s3_bucket: S3 bucket name
            s3_path: Path within bucket
            aws_key_id: AWS access key ID
            aws_secret_key: AWS secret access key
            file_format: File format (JSON, CSV, PARQUET)
            
        Returns:
            True if successful
        """
        s3_url = f"s3://{s3_bucket}/{s3_path}" if s3_path else f"s3://{s3_bucket}"
        
        query = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = '{s3_url}'
        """
        
        if aws_key_id and aws_secret_key:
            query += f"""
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_key_id}'
                AWS_SECRET_KEY = '{aws_secret_key}'
            )
            """
        
        query += f"""
        FILE_FORMAT = (TYPE = {file_format});
        """
        
        try:
            self.execute_query(query)
            logger.info(f"Created stage {stage_name} pointing to {s3_url}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating stage: {str(e)}")
            raise
    
    def copy_into_table(
        self,
        table_name: str,
        stage_name: str,
        file_pattern: Optional[str] = None,
        file_format: str = 'JSON',
        on_error: str = 'CONTINUE'
    ) -> Dict[str, Any]:
        """
        Copy data from stage into table
        
        Args:
            table_name: Target table name
            stage_name: Source stage name
            file_pattern: Pattern to match files (e.g., '.*\.json')
            file_format: File format
            on_error: Error handling (CONTINUE, ABORT_STATEMENT)
            
        Returns:
            Dictionary with copy statistics
        """
        query = f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        """
        
        if file_pattern:
            query += f"\nPATTERN = '{file_pattern}'"
        
        query += f"""
        FILE_FORMAT = (TYPE = {file_format})
        ON_ERROR = {on_error};
        """
        
        try:
            results = self.execute_query(query)
            
            # Parse results
            stats = {
                'rows_loaded': 0,
                'errors_seen': 0,
                'files_loaded': 0
            }
            
            for row in results:
                if len(row) >= 3:
                    stats['files_loaded'] += 1
                    stats['rows_loaded'] += row[1] if row[1] else 0
                    stats['errors_seen'] += row[5] if len(row) > 5 and row[5] else 0
            
            logger.info(f"Copied data into {table_name}: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error copying data into table: {str(e)}")
            raise
    
    def create_table_from_json(
        self,
        table_name: str,
        json_schema: Dict[str, str],
        add_metadata_columns: bool = True
    ) -> bool:
        """
        Create a table with schema derived from JSON structure
        
        Args:
            table_name: Name of table to create
            json_schema: Dictionary mapping column names to data types
            add_metadata_columns: Add audit columns
            
        Returns:
            True if successful
        """
        columns = []
        for col_name, col_type in json_schema.items():
            columns.append(f"{col_name} {col_type}")
        
        if add_metadata_columns:
            columns.extend([
                "loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()",
                "source_file VARCHAR(500)"
            ])
        
        columns_str = ',\n    '.join(columns)
        
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_str}
        );
        """
        
        try:
            self.execute_query(query)
            logger.info(f"Created table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate a table
        
        Args:
            table_name: Name of table to truncate
            
        Returns:
            True if successful
        """
        query = f"TRUNCATE TABLE {table_name};"
        
        try:
            self.execute_query(query)
            logger.info(f"Truncated table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error truncating table: {str(e)}")
            raise
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get row count for a table
        
        Args:
            table_name: Table name
            
        Returns:
            Number of rows
        """
        query = f"SELECT COUNT(*) FROM {table_name};"
        results = self.execute_query(query)
        return results[0][0] if results else 0
    
    def run_sql_file(self, sql_file_path: str) -> bool:
        """
        Execute SQL commands from a file
        
        Args:
            sql_file_path: Path to SQL file
            
        Returns:
            True if successful
        """
        try:
            with open(sql_file_path, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolons and execute each statement
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            for statement in statements:
                self.execute_query(statement)
            
            logger.info(f"Executed {len(statements)} statements from {sql_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error running SQL file: {str(e)}")
            raise
    
    def merge_data(
        self,
        target_table: str,
        source_table: str,
        merge_keys: List[str],
        update_columns: List[str]
    ) -> Dict[str, int]:
        """
        Merge data from source into target table
        
        Args:
            target_table: Target table name
            source_table: Source table name
            merge_keys: Columns to join on
            update_columns: Columns to update
            
        Returns:
            Dictionary with merge statistics
        """
        # Build MERGE statement
        join_condition = ' AND '.join([
            f"target.{key} = source.{key}" for key in merge_keys
        ])
        
        update_set = ', '.join([
            f"target.{col} = source.{col}" for col in update_columns
        ])
        
        insert_columns = ', '.join(merge_keys + update_columns)
        insert_values = ', '.join([f"source.{col}" for col in merge_keys + update_columns])
        
        query = f"""
        MERGE INTO {target_table} AS target
        USING {source_table} AS source
        ON {join_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
        """
        
        try:
            results = self.execute_query(query)
            logger.info(f"Merged data from {source_table} into {target_table}")
            return {'merged': len(results)}
            
        except Exception as e:
            logger.error(f"Error merging data: {str(e)}")
            raise
