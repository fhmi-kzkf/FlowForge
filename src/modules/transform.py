"""
Data transformation module for FlowForge ETL Platform
Handles various data cleaning and transformation operations
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime
import re

from utils.logger import logger
from utils.helpers import get_dataframe_info

class DataTransformer:
    """Handles data transformation operations"""
    
    def __init__(self):
        self.transformation_history = []
    
    def log_transformation(self, operation: str, details: str, rows_before: int, 
                          rows_after: int, cols_before: int, cols_after: int):
        """Log transformation operation"""
        transformation = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": operation,
            "details": details,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "cols_before": cols_before,
            "cols_after": cols_after,
            "rows_changed": rows_after - rows_before,
            "cols_changed": cols_after - cols_before
        }
        self.transformation_history.append(transformation)
        logger.log_etl_step("TRANSFORM", f"{operation}: {details}")
    
    def remove_duplicates(self, df: pd.DataFrame, columns: List[str] = None, 
                         keep: str = 'first') -> Tuple[pd.DataFrame, str]:
        """
        Remove duplicate rows
        
        Args:
            df: Input DataFrame
            columns: Specific columns to check for duplicates (None for all columns)
            keep: Which duplicates to keep ('first', 'last', False)
        
        Returns:
            Tuple of (transformed DataFrame, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            
            if columns:
                df_result = df.drop_duplicates(subset=columns, keep=keep)
                details = f"Based on columns: {', '.join(columns)}, keep='{keep}'"
            else:
                df_result = df.drop_duplicates(keep=keep)
                details = f"All columns, keep='{keep}'"
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            duplicates_removed = rows_before - rows_after
            
            self.log_transformation(
                "REMOVE_DUPLICATES", details, 
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Removed {duplicates_removed} duplicate rows"
            
        except Exception as e:
            error_msg = f"Error removing duplicates: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def handle_missing_values(self, df: pd.DataFrame, method: str, 
                            columns: List[str] = None, fill_value: Any = None) -> Tuple[pd.DataFrame, str]:
        """
        Handle missing values in DataFrame
        
        Args:
            df: Input DataFrame
            method: Method to handle missing values ('drop', 'fill_mean', 'fill_median', 'fill_mode', 'fill_value', 'forward_fill', 'backward_fill')
            columns: Specific columns to apply operation (None for all columns)
            fill_value: Value to fill when method is 'fill_value'
        
        Returns:
            Tuple of (transformed DataFrame, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            df_result = df.copy()
            
            target_columns = columns if columns else df.columns.tolist()
            missing_before = df[target_columns].isnull().sum().sum()
            
            if method == 'drop':
                df_result = df_result.dropna(subset=target_columns)
                details = f"Dropped rows with missing values in {len(target_columns)} columns"
                
            elif method == 'fill_mean':
                numeric_cols = df_result[target_columns].select_dtypes(include=[np.number]).columns
                df_result[numeric_cols] = df_result[numeric_cols].fillna(df_result[numeric_cols].mean())
                details = f"Filled with mean for {len(numeric_cols)} numeric columns"
                
            elif method == 'fill_median':
                numeric_cols = df_result[target_columns].select_dtypes(include=[np.number]).columns
                df_result[numeric_cols] = df_result[numeric_cols].fillna(df_result[numeric_cols].median())
                details = f"Filled with median for {len(numeric_cols)} numeric columns"
                
            elif method == 'fill_mode':
                for col in target_columns:
                    mode_value = df_result[col].mode()
                    if not mode_value.empty:
                        df_result[col] = df_result[col].fillna(mode_value[0])
                details = f"Filled with mode for {len(target_columns)} columns"
                
            elif method == 'fill_value':
                df_result[target_columns] = df_result[target_columns].fillna(fill_value)
                details = f"Filled with '{fill_value}' for {len(target_columns)} columns"
                
            elif method == 'forward_fill':
                df_result[target_columns] = df_result[target_columns].fillna(method='ffill')
                details = f"Forward fill for {len(target_columns)} columns"
                
            elif method == 'backward_fill':
                df_result[target_columns] = df_result[target_columns].fillna(method='bfill')
                details = f"Backward fill for {len(target_columns)} columns"
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            missing_after = df_result[target_columns].isnull().sum().sum()
            
            self.log_transformation(
                "HANDLE_MISSING", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Handled {missing_before - missing_after} missing values using {method}"
            
        except Exception as e:
            error_msg = f"Error handling missing values: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def filter_data(self, df: pd.DataFrame, column: str, operator: str, 
                   value: Union[str, int, float]) -> Tuple[pd.DataFrame, str]:
        """
        Filter DataFrame based on column conditions
        
        Args:
            df: Input DataFrame
            column: Column name to filter on
            operator: Comparison operator ('==', '!=', '>', '<', '>=', '<=', 'contains', 'startswith', 'endswith')
            value: Value to compare against
        
        Returns:
            Tuple of (filtered DataFrame, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            
            if column not in df.columns:
                return df, f"Column '{column}' not found in DataFrame"
            
            if operator == '==':
                df_result = df[df[column] == value]
            elif operator == '!=':
                df_result = df[df[column] != value]
            elif operator == '>':
                df_result = df[df[column] > value]
            elif operator == '<':
                df_result = df[df[column] < value]
            elif operator == '>=':
                df_result = df[df[column] >= value]
            elif operator == '<=':
                df_result = df[df[column] <= value]
            elif operator == 'contains':
                df_result = df[df[column].astype(str).str.contains(str(value), na=False)]
            elif operator == 'startswith':
                df_result = df[df[column].astype(str).str.startswith(str(value), na=False)]
            elif operator == 'endswith':
                df_result = df[df[column].astype(str).str.endswith(str(value), na=False)]
            else:
                return df, f"Unsupported operator: {operator}"
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            details = f"Column '{column}' {operator} '{value}'"
            self.log_transformation(
                "FILTER_DATA", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Filtered to {rows_after} rows where {details}"
            
        except Exception as e:
            error_msg = f"Error filtering data: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def sort_data(self, df: pd.DataFrame, columns: List[str], 
                 ascending: List[bool] = None) -> Tuple[pd.DataFrame, str]:
        """
        Sort DataFrame by specified columns
        
        Args:
            df: Input DataFrame
            columns: List of column names to sort by
            ascending: List of boolean values for sort order (True for ascending)
        
        Returns:
            Tuple of (sorted DataFrame, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            
            # Validate columns
            invalid_cols = [col for col in columns if col not in df.columns]
            if invalid_cols:
                return df, f"Columns not found: {', '.join(invalid_cols)}"
            
            if ascending is None:
                ascending = [True] * len(columns)
            elif len(ascending) != len(columns):
                return df, "Length of ascending list must match columns list"
            
            df_result = df.sort_values(by=columns, ascending=ascending)
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            sort_details = []
            for col, asc in zip(columns, ascending):
                sort_details.append(f"{col} ({'asc' if asc else 'desc'})")
            
            details = f"Sorted by: {', '.join(sort_details)}"
            self.log_transformation(
                "SORT_DATA", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Sorted data by {len(columns)} columns"
            
        except Exception as e:
            error_msg = f"Error sorting data: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def rename_columns(self, df: pd.DataFrame, 
                      column_mapping: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
        """
        Rename columns in DataFrame
        
        Args:
            df: Input DataFrame
            column_mapping: Dictionary mapping old names to new names
        
        Returns:
            Tuple of (DataFrame with renamed columns, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            
            # Validate columns exist
            invalid_cols = [col for col in column_mapping.keys() if col not in df.columns]
            if invalid_cols:
                return df, f"Columns not found: {', '.join(invalid_cols)}"
            
            df_result = df.rename(columns=column_mapping)
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            details = f"Renamed {len(column_mapping)} columns"
            self.log_transformation(
                "RENAME_COLUMNS", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Renamed {len(column_mapping)} columns"
            
        except Exception as e:
            error_msg = f"Error renaming columns: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def drop_columns(self, df: pd.DataFrame, columns: List[str]) -> Tuple[pd.DataFrame, str]:
        """
        Drop columns from DataFrame
        
        Args:
            df: Input DataFrame
            columns: List of column names to drop
        
        Returns:
            Tuple of (DataFrame without specified columns, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            
            # Validate columns exist
            invalid_cols = [col for col in columns if col not in df.columns]
            if invalid_cols:
                return df, f"Columns not found: {', '.join(invalid_cols)}"
            
            df_result = df.drop(columns=columns)
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            details = f"Dropped columns: {', '.join(columns)}"
            self.log_transformation(
                "DROP_COLUMNS", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Dropped {len(columns)} columns"
            
        except Exception as e:
            error_msg = f"Error dropping columns: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def convert_data_types(self, df: pd.DataFrame, 
                          type_mapping: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
        """
        Convert column data types
        
        Args:
            df: Input DataFrame
            type_mapping: Dictionary mapping column names to target data types
        
        Returns:
            Tuple of (DataFrame with converted types, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            df_result = df.copy()
            
            conversion_errors = []
            successful_conversions = []
            
            for column, target_type in type_mapping.items():
                if column not in df.columns:
                    conversion_errors.append(f"Column '{column}' not found")
                    continue
                
                try:
                    if target_type == 'int':
                        df_result[column] = pd.to_numeric(df_result[column], errors='coerce').astype('Int64')
                    elif target_type == 'float':
                        df_result[column] = pd.to_numeric(df_result[column], errors='coerce')
                    elif target_type == 'string':
                        df_result[column] = df_result[column].astype(str)
                    elif target_type == 'datetime':
                        df_result[column] = pd.to_datetime(df_result[column], errors='coerce')
                    elif target_type == 'boolean':
                        df_result[column] = df_result[column].astype(bool)
                    elif target_type == 'category':
                        df_result[column] = df_result[column].astype('category')
                    
                    successful_conversions.append(f"{column} → {target_type}")
                    
                except Exception as e:
                    conversion_errors.append(f"Failed to convert '{column}' to {target_type}: {str(e)}")
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            details = f"Converted {len(successful_conversions)} columns"
            self.log_transformation(
                "CONVERT_TYPES", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            message = f"Successfully converted {len(successful_conversions)} columns"
            if conversion_errors:
                message += f". Errors: {'; '.join(conversion_errors)}"
            
            return df_result, message
            
        except Exception as e:
            error_msg = f"Error converting data types: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def create_calculated_column(self, df: pd.DataFrame, new_column: str, 
                               expression: str) -> Tuple[pd.DataFrame, str]:
        """
        Create a new calculated column based on expression
        
        Args:
            df: Input DataFrame
            new_column: Name for the new column
            expression: Python expression using column names
        
        Returns:
            Tuple of (DataFrame with new column, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            df_result = df.copy()
            
            # Replace column names in expression with df['column'] format
            for col in df.columns:
                expression = expression.replace(col, f"df_result['{col}']")
            
            # Evaluate expression safely
            df_result[new_column] = eval(expression)
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            details = f"Created '{new_column}' with expression: {expression}"
            self.log_transformation(
                "CREATE_COLUMN", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Created calculated column '{new_column}'"
            
        except Exception as e:
            error_msg = f"Error creating calculated column: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def text_operations(self, df: pd.DataFrame, column: str, operation: str, 
                       **kwargs) -> Tuple[pd.DataFrame, str]:
        """
        Perform text operations on string columns
        
        Args:
            df: Input DataFrame
            column: Column name to operate on
            operation: Type of operation ('upper', 'lower', 'title', 'strip', 'replace', 'extract')
            **kwargs: Additional parameters for specific operations
        
        Returns:
            Tuple of (transformed DataFrame, message)
        """
        try:
            rows_before, cols_before = len(df), len(df.columns)
            
            if column not in df.columns:
                return df, f"Column '{column}' not found"
            
            df_result = df.copy()
            
            if operation == 'upper':
                df_result[column] = df_result[column].astype(str).str.upper()
                details = f"Converted '{column}' to uppercase"
                
            elif operation == 'lower':
                df_result[column] = df_result[column].astype(str).str.lower()
                details = f"Converted '{column}' to lowercase"
                
            elif operation == 'title':
                df_result[column] = df_result[column].astype(str).str.title()
                details = f"Converted '{column}' to title case"
                
            elif operation == 'strip':
                df_result[column] = df_result[column].astype(str).str.strip()
                details = f"Stripped whitespace from '{column}'"
                
            elif operation == 'replace':
                old_value = kwargs.get('old_value', '')
                new_value = kwargs.get('new_value', '')
                df_result[column] = df_result[column].astype(str).str.replace(old_value, new_value)
                details = f"Replaced '{old_value}' with '{new_value}' in '{column}'"
                
            elif operation == 'extract':
                pattern = kwargs.get('pattern', '')
                df_result[f"{column}_extracted"] = df_result[column].astype(str).str.extract(pattern)
                details = f"Extracted pattern '{pattern}' from '{column}'"
            
            else:
                return df, f"Unsupported text operation: {operation}"
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            self.log_transformation(
                "TEXT_OPERATION", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            return df_result, f"Applied {operation} operation to '{column}'"
            
        except Exception as e:
            error_msg = f"Error in text operation: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def fix_column_typos(self, df: pd.DataFrame, corrections: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
        """Fix typos in column names with suggested corrections"""
        try:
            rows_before, cols_before = len(df), len(df.columns)
            
            # Validate that all columns to fix exist
            invalid_cols = [col for col in corrections.keys() if col not in df.columns]
            if invalid_cols:
                return df, f"Error: Columns not found: {', '.join(invalid_cols)}"
            
            # Apply corrections
            df_result = df.rename(columns=corrections)
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            details = f"Fixed column typos: {corrections}"
            self.log_transformation(
                "FIX_COLUMN_TYPOS", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            message = f"Successfully fixed {len(corrections)} column name typos"
            return df_result, message
            
        except Exception as e:
            error_msg = f"Error fixing column typos: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def fix_data_typos(self, df: pd.DataFrame, column: str, corrections: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
        """Fix typos in data values within a specific column"""
        try:
            if column not in df.columns:
                return df, f"Error: Column '{column}' not found"
            
            rows_before, cols_before = len(df), len(df.columns)
            
            # Apply corrections to the specific column
            df_result = df.copy()
            df_result[column] = df_result[column].replace(corrections)
            
            rows_after, cols_after = len(df_result), len(df_result.columns)
            
            # Count how many values were actually changed
            changes_made = 0
            for old_val, new_val in corrections.items():
                changes_made += (df[column] == old_val).sum()
            
            details = f"Fixed data typos in '{column}': {corrections} ({changes_made} values changed)"
            self.log_transformation(
                "FIX_DATA_TYPOS", details,
                rows_before, rows_after, cols_before, cols_after
            )
            
            message = f"Successfully fixed {len(corrections)} typos in column '{column}' ({changes_made} values changed)"
            return df_result, message
            
        except Exception as e:
            error_msg = f"Error fixing data typos: {str(e)}"
            logger.log_etl_step("ERROR", error_msg)
            return df, error_msg
    
    def suggest_typo_fixes(self, df: pd.DataFrame, column: str = None) -> Dict[str, Any]:
        """Suggest potential typo fixes for column names or data values"""
        from difflib import get_close_matches
        
        suggestions = {"column_suggestions": {}, "data_suggestions": {}}
        
        try:
            # Suggest column name fixes
            if column is None:
                # Check all column names for potential typos
                common_words = ['name', 'id', 'date', 'time', 'price', 'amount', 'count', 'total', 'value', 'description']
                for col in df.columns:
                    # Look for similar common words
                    matches = get_close_matches(col.lower(), common_words, n=3, cutoff=0.6)
                    if matches and col.lower() not in common_words:
                        suggestions["column_suggestions"][col] = matches
            
            # Suggest data value fixes for specific column
            if column and column in df.columns:
                if df[column].dtype == 'object':  # Text columns only
                    value_counts = df[column].value_counts()
                    unique_values = value_counts.index.tolist()
                    
                    # Find potential typos (values that appear infrequently and are similar to more common ones)
                    for value in unique_values:
                        if value_counts[value] <= 2:  # Infrequent values might be typos
                            matches = get_close_matches(str(value), 
                                                      [str(v) for v in unique_values if value_counts[v] > 2], 
                                                      n=2, cutoff=0.8)
                            if matches:
                                suggestions["data_suggestions"][str(value)] = matches
            
            return suggestions
            
        except Exception as e:
            logger.log_etl_step("ERROR", f"Error suggesting typo fixes: {str(e)}")
            return suggestions
    
    def get_transformation_history(self) -> List[Dict[str, Any]]:
        """Get the transformation history"""
        return self.transformation_history
    
    def clear_history(self):
        """Clear the transformation history"""
        self.transformation_history = []

# Global transformer instance
transformer = DataTransformer()