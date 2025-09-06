"""
Data loading module for FlowForge ETL Platform
Handles saving processed data to various formats and destinations
"""

import pandas as pd
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime
import io

from utils.config import EXPORTS_DIR, DEFAULT_DB_PATH
from utils.logger import logger

class DataLoader:
    """Handles data loading/saving operations"""
    
    def __init__(self):
        self.export_history = []
    
    def log_export(self, operation: str, destination: str, rows: int, cols: int, file_size: float = None):
        """Log export operation"""
        export_info = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": operation,
            "destination": destination,
            "rows": rows,
            "columns": cols,
            "file_size_mb": file_size
        }
        self.export_history.append(export_info)
        logger.log_etl_step("LOAD", f"{operation} to {destination}: {rows} rows, {cols} cols")
    
    def save_to_csv(self, df: pd.DataFrame, filename: str, include_index: bool = False) -> Tuple[bool, str, str]:
        """
        Save DataFrame to CSV file
        
        Args:
            df: DataFrame to save
            filename: Name of the file (without extension)
            include_index: Whether to include row index
        
        Returns:
            Tuple of (success, message, file_path)
        """
        try:
            if df.empty:
                return False, "Cannot save empty DataFrame", ""
            
            # Ensure exports directory exists
            EXPORTS_DIR.mkdir(exist_ok=True)
            
            # Create file path
            file_path = EXPORTS_DIR / f"{filename}.csv"
            
            # Save to CSV
            df.to_csv(file_path, index=include_index)
            
            # Calculate file size
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            
            self.log_export("CSV_EXPORT", str(file_path), len(df), len(df.columns), file_size_mb)
            
            return True, f"Successfully saved to {file_path}", str(file_path)
            
        except Exception as e:
            error_msg = f"Error saving to CSV: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return False, error_msg, ""
    
    def save_to_excel(self, df: pd.DataFrame, filename: str, sheet_name: str = "Sheet1", 
                     include_index: bool = False) -> Tuple[bool, str, str]:
        """
        Save DataFrame to Excel file
        
        Args:
            df: DataFrame to save
            filename: Name of the file (without extension)
            sheet_name: Name of the Excel sheet
            include_index: Whether to include row index
        
        Returns:
            Tuple of (success, message, file_path)
        """
        try:
            if df.empty:
                return False, "Cannot save empty DataFrame", ""
            
            # Ensure exports directory exists
            EXPORTS_DIR.mkdir(exist_ok=True)
            
            # Create file path
            file_path = EXPORTS_DIR / f"{filename}.xlsx"
            
            # Save to Excel
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=include_index)
            
            # Calculate file size
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            
            self.log_export("EXCEL_EXPORT", str(file_path), len(df), len(df.columns), file_size_mb)
            
            return True, f"Successfully saved to {file_path}", str(file_path)
            
        except Exception as e:
            error_msg = f"Error saving to Excel: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return False, error_msg, ""
    
    def save_to_json(self, df: pd.DataFrame, filename: str, orient: str = "records") -> Tuple[bool, str, str]:
        """
        Save DataFrame to JSON file
        
        Args:
            df: DataFrame to save
            filename: Name of the file (without extension)
            orient: JSON orientation ('records', 'index', 'values', 'columns')
        
        Returns:
            Tuple of (success, message, file_path)
        """
        try:
            if df.empty:
                return False, "Cannot save empty DataFrame", ""
            
            # Ensure exports directory exists
            EXPORTS_DIR.mkdir(exist_ok=True)
            
            # Create file path
            file_path = EXPORTS_DIR / f"{filename}.json"
            
            # Save to JSON
            df.to_json(file_path, orient=orient, date_format='iso')
            
            # Calculate file size
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            
            self.log_export("JSON_EXPORT", str(file_path), len(df), len(df.columns), file_size_mb)
            
            return True, f"Successfully saved to {file_path}", str(file_path)
            
        except Exception as e:
            error_msg = f"Error saving to JSON: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return False, error_msg, ""
    
    def save_to_sqlite(self, df: pd.DataFrame, table_name: str, db_path: str = None, 
                      if_exists: str = "replace") -> Tuple[bool, str]:
        """
        Save DataFrame to SQLite database
        
        Args:
            df: DataFrame to save
            table_name: Name of the table to create/update
            db_path: Path to SQLite database (uses default if None)
            if_exists: What to do if table exists ('fail', 'replace', 'append')
        
        Returns:
            Tuple of (success, message)
        """
        try:
            if df.empty:
                return False, "Cannot save empty DataFrame"
            
            # Use default database path if none provided
            if db_path is None:
                EXPORTS_DIR.mkdir(exist_ok=True)
                db_path = str(DEFAULT_DB_PATH)
            
            # Connect to database
            conn = sqlite3.connect(db_path)
            
            try:
                # Save DataFrame to SQLite
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
                
                # Verify the save
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                if row_count != len(df):
                    return False, f"Row count mismatch: expected {len(df)}, got {row_count}"
                
                self.log_export("SQLITE_EXPORT", f"{db_path}:{table_name}", len(df), len(df.columns))
                
                return True, f"Successfully saved {len(df)} rows to table '{table_name}' in {db_path}"
                
            finally:
                conn.close()
                
        except Exception as e:
            error_msg = f"Error saving to SQLite: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return False, error_msg
    
    def create_download_data(self, df: pd.DataFrame, file_format: str, 
                           **kwargs) -> Tuple[Optional[bytes], str, str]:
        """
        Create downloadable data in memory
        
        Args:
            df: DataFrame to convert
            file_format: Format for download ('csv', 'xlsx', 'json')
            **kwargs: Additional parameters for specific formats
        
        Returns:
            Tuple of (data_bytes, mime_type, suggested_filename)
        """
        try:
            if df.empty:
                return None, "", "Cannot create download for empty DataFrame"
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if file_format.lower() == "csv":
                # Create CSV in memory
                output = io.StringIO()
                df.to_csv(output, index=kwargs.get('include_index', False))
                data = output.getvalue().encode('utf-8')
                mime_type = "text/csv"
                filename = f"flowforge_data_{timestamp}.csv"
                
            elif file_format.lower() == "xlsx":
                # Create Excel in memory
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name=kwargs.get('sheet_name', 'Sheet1'), 
                              index=kwargs.get('include_index', False))
                data = output.getvalue()
                mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                filename = f"flowforge_data_{timestamp}.xlsx"
                
            elif file_format.lower() == "json":
                # Create JSON in memory
                json_str = df.to_json(orient=kwargs.get('orient', 'records'), date_format='iso')
                data = json_str.encode('utf-8')
                mime_type = "application/json"
                filename = f"flowforge_data_{timestamp}.json"
                
            else:
                return None, "", f"Unsupported format: {file_format}"
            
            logger.log_etl_step("LOAD", f"Created download data: {file_format.upper()}, {len(data)} bytes")
            
            return data, mime_type, filename
            
        except Exception as e:
            error_msg = f"Error creating download data: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return None, "", error_msg
    
    def get_export_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary of data ready for export
        
        Args:
            df: DataFrame to summarize
        
        Returns:
            Dictionary with export summary information
        """
        try:
            if df.empty:
                return {"error": "No data available for export"}
            
            summary = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
                "column_types": df.dtypes.value_counts().to_dict(),
                "null_values": df.isnull().sum().sum(),
                "estimated_csv_size_mb": len(df.to_csv(index=False)) / (1024 * 1024),
                "numeric_columns": len(df.select_dtypes(include=['number']).columns),
                "text_columns": len(df.select_dtypes(include=['object']).columns),
                "datetime_columns": len(df.select_dtypes(include=['datetime']).columns)
            }
            
            return summary
            
        except Exception as e:
            return {"error": f"Error creating summary: {str(e)}"}
    
    def list_saved_files(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        List all saved files in the exports directory
        
        Returns:
            Dictionary with file information grouped by type
        """
        try:
            if not EXPORTS_DIR.exists():
                return {"csv": [], "excel": [], "json": [], "sqlite": []}
            
            files = {"csv": [], "excel": [], "json": [], "sqlite": []}
            
            for file_path in EXPORTS_DIR.iterdir():
                if file_path.is_file():
                    file_info = {
                        "name": file_path.name,
                        "path": str(file_path),
                        "size_mb": file_path.stat().st_size / (1024 * 1024),
                        "modified": datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    if file_path.suffix.lower() == ".csv":
                        files["csv"].append(file_info)
                    elif file_path.suffix.lower() in [".xlsx", ".xls"]:
                        files["excel"].append(file_info)
                    elif file_path.suffix.lower() == ".json":
                        files["json"].append(file_info)
                    elif file_path.suffix.lower() in [".db", ".sqlite", ".sqlite3"]:
                        files["sqlite"].append(file_info)
            
            # Sort by modification time (newest first)
            for file_type in files:
                files[file_type].sort(key=lambda x: x["modified"], reverse=True)
            
            return files
            
        except Exception as e:
            logger.log_etl_step("ERROR", f"Error listing saved files: {str(e)}")
            return {"csv": [], "excel": [], "json": [], "sqlite": []}
    
    def get_export_history(self) -> List[Dict[str, Any]]:
        """Get the export history"""
        return self.export_history
    
    def clear_export_history(self):
        """Clear the export history"""
        self.export_history = []

# Global loader instance
loader = DataLoader()