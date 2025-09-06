"""
Data extraction module for FlowForge ETL Platform
Handles extraction from various data sources: CSV, Excel, JSON, APIs, SQLite
"""

import pandas as pd
import requests
import sqlite3
import json
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
from io import StringIO

from utils.config import DEFAULT_TIMEOUT, MAX_API_RETRIES, SAMPLES_DIR
from utils.logger import logger
from utils.helpers import safe_json_loads

class DataExtractor:
    """Handles data extraction from various sources"""
    
    def __init__(self):
        self.supported_formats = ["csv", "xlsx", "xls", "json", "sqlite", "api"]
    
    def extract_from_file(self, uploaded_file, file_type: str = None) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Extract data from uploaded file
        
        Args:
            uploaded_file: Streamlit uploaded file object
            file_type: Optional file type override
            
        Returns:
            Tuple of (DataFrame or None, message)
        """
        try:
            if uploaded_file is None:
                return None, "No file provided"
            
            # Determine file type from extension if not provided
            if file_type is None:
                file_type = uploaded_file.name.split('.')[-1].lower()
            
            logger.log_etl_step("EXTRACT", f"Starting extraction from {file_type} file: {uploaded_file.name}")
            
            # Extract based on file type
            if file_type == "csv":
                df = pd.read_csv(uploaded_file)
            elif file_type in ["xlsx", "xls"]:
                df = pd.read_excel(uploaded_file)
            elif file_type == "json":
                json_data = json.load(uploaded_file)
                df = pd.json_normalize(json_data)
            else:
                return None, f"Unsupported file type: {file_type}"
            
            # Validate extracted data
            if df.empty:
                return None, "Extracted data is empty"
            
            message = f"Successfully extracted {len(df)} rows and {len(df.columns)} columns from {uploaded_file.name}"
            logger.log_data_info("FILE_EXTRACT", len(df), len(df.columns), uploaded_file.name)
            
            return df, message
            
        except Exception as e:
            error_msg = f"Error extracting from file: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
    
    def extract_from_csv_text(self, csv_text: str, delimiter: str = ",") -> Tuple[Optional[pd.DataFrame], str]:
        """
        Extract data from CSV text input
        
        Args:
            csv_text: Raw CSV text
            delimiter: CSV delimiter
            
        Returns:
            Tuple of (DataFrame or None, message)
        """
        try:
            if not csv_text.strip():
                return None, "No CSV text provided"
            
            logger.log_etl_step("EXTRACT", f"Extracting data from CSV text input")
            
            # Use StringIO to read CSV from text
            csv_io = StringIO(csv_text)
            df = pd.read_csv(csv_io, delimiter=delimiter)
            
            if df.empty:
                return None, "Parsed CSV is empty"
            
            message = f"Successfully parsed {len(df)} rows and {len(df.columns)} columns from CSV text"
            logger.log_data_info("CSV_TEXT_EXTRACT", len(df), len(df.columns))
            
            return df, message
            
        except Exception as e:
            error_msg = f"Error parsing CSV text: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
    
    def extract_from_api(self, url: str, headers: Dict[str, str] = None, 
                        params: Dict[str, Any] = None) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Extract data from API endpoint
        
        Args:
            url: API endpoint URL
            headers: Optional HTTP headers
            params: Optional query parameters
            
        Returns:
            Tuple of (DataFrame or None, message)
        """
        try:
            if not url.strip():
                return None, "No URL provided"
            
            logger.log_etl_step("EXTRACT", f"Fetching data from API: {url}")
            
            # Make API request
            response = requests.get(
                url, 
                headers=headers or {}, 
                params=params or {},
                timeout=DEFAULT_TIMEOUT
            )
            
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # If it's a dict, try to find a list within it
                if len(data) == 1 and isinstance(list(data.values())[0], list):
                    df = pd.DataFrame(list(data.values())[0])
                else:
                    df = pd.json_normalize(data)
            else:
                return None, "API response is not in a supported format"
            
            if df.empty:
                return None, "API returned empty data"
            
            message = f"Successfully fetched {len(df)} rows and {len(df.columns)} columns from API"
            logger.log_data_info("API_EXTRACT", len(df), len(df.columns), url)
            
            return df, message
            
        except requests.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse API response as JSON: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"Error extracting from API: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
    
    def extract_from_sqlite(self, db_path: str, table_name: str = None, 
                           query: str = None) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Extract data from SQLite database
        
        Args:
            db_path: Path to SQLite database file
            table_name: Name of table to extract (if not using custom query)
            query: Custom SQL query (overrides table_name)
            
        Returns:
            Tuple of (DataFrame or None, message)
        """
        try:
            if not Path(db_path).exists():
                return None, f"Database file not found: {db_path}"
            
            logger.log_etl_step("EXTRACT", f"Connecting to SQLite database: {db_path}")
            
            # Connect to database
            conn = sqlite3.connect(db_path)
            
            try:
                if query:
                    # Use custom query
                    df = pd.read_sql_query(query, conn)
                    source_info = f"query: {query[:50]}..."
                elif table_name:
                    # Extract entire table
                    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                    source_info = f"table: {table_name}"
                else:
                    return None, "Either table_name or query must be provided"
                
                if df.empty:
                    return None, f"No data found for {source_info}"
                
                message = f"Successfully extracted {len(df)} rows and {len(df.columns)} columns from {source_info}"
                logger.log_data_info("SQLITE_EXTRACT", len(df), len(df.columns), db_path)
                
                return df, message
                
            finally:
                conn.close()
                
        except sqlite3.Error as e:
            error_msg = f"SQLite error: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"Error extracting from SQLite: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
    
    def list_sqlite_tables(self, db_path: str) -> Tuple[Optional[list], str]:
        """
        List available tables in SQLite database
        
        Args:
            db_path: Path to SQLite database file
            
        Returns:
            Tuple of (list of table names or None, message)
        """
        try:
            if not Path(db_path).exists():
                return None, f"Database file not found: {db_path}"
            
            conn = sqlite3.connect(db_path)
            
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                
                return tables, f"Found {len(tables)} tables in database"
                
            finally:
                conn.close()
                
        except sqlite3.Error as e:
            error_msg = f"SQLite error: {str(e)}"
            return None, error_msg
        except Exception as e:
            error_msg = f"Error listing tables: {str(e)}"
            return None, error_msg
    
    def extract_sample_data(self, sample_name: str) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Extract data from built-in sample datasets
        
        Args:
            sample_name: Name of the sample dataset
            
        Returns:
            Tuple of (DataFrame or None, message)
        """
        try:
            sample_path = SAMPLES_DIR / f"{sample_name}.csv"
            
            if not sample_path.exists():
                return None, f"Sample dataset '{sample_name}' not found"
            
            logger.log_etl_step("EXTRACT", f"Loading sample dataset: {sample_name}")
            
            df = pd.read_csv(sample_path)
            
            if df.empty:
                return None, f"Sample dataset '{sample_name}' is empty"
            
            message = f"Successfully loaded sample '{sample_name}': {len(df)} rows, {len(df.columns)} columns"
            logger.log_data_info("SAMPLE_EXTRACT", len(df), len(df.columns), sample_name)
            
            return df, message
            
        except Exception as e:
            error_msg = f"Error loading sample dataset: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, error_msg
    
    def get_popular_apis(self) -> Dict[str, Dict[str, str]]:
        """
        Get list of popular public APIs for demonstration
        
        Returns:
            Dictionary of API configurations
        """
        return {
            "JSONPlaceholder Posts": {
                "url": "https://jsonplaceholder.typicode.com/posts",
                "description": "Sample blog posts data",
                "type": "REST API"
            },
            "JSONPlaceholder Users": {
                "url": "https://jsonplaceholder.typicode.com/users",
                "description": "Sample user data",
                "type": "REST API"
            },
            "JSONPlaceholder Comments": {
                "url": "https://jsonplaceholder.typicode.com/comments", 
                "description": "Sample comments data",
                "type": "REST API"
            },
            "HTTPBin": {
                "url": "https://httpbin.org/json",
                "description": "Simple JSON response",
                "type": "Test API"
            }
        }

# Global extractor instance
extractor = DataExtractor()