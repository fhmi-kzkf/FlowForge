"""
Helper functions for FlowForge ETL Platform
"""

import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Optional, Tuple
import json
from datetime import datetime
from utils.config import THEME_COLORS
from utils.logger import logger

def display_success_message(message: str):
    """Display success message with consistent styling"""
    st.success(f"✅ {message}")

def display_error_message(message: str):
    """Display error message with consistent styling"""
    st.error(f"❌ {message}")

def display_warning_message(message: str):
    """Display warning message with consistent styling"""
    st.warning(f"⚠️ {message}")

def display_info_message(message: str):
    """Display info message with consistent styling"""
    st.info(f"ℹ️ {message}")

def get_dataframe_info(df: pd.DataFrame) -> Dict[str, Any]:
    """Get comprehensive dataframe information"""
    try:
        null_sum = df.isnull().sum()
        info = {
            "rows": len(df),
            "columns": len(df.columns),
            "memory_usage": df.memory_usage(deep=True).sum() / (1024 * 1024),  # MB
            "dtypes": df.dtypes.to_dict(),
            "null_counts": null_sum.sum(),  # Total null count for metric display
            "null_counts_by_column": null_sum.to_dict(),  # Per-column null counts for detailed analysis
            "duplicate_rows": df.duplicated().sum(),
            "numeric_columns": df.select_dtypes(include=['number']).columns.tolist(),
            "categorical_columns": df.select_dtypes(include=['object']).columns.tolist(),
            "datetime_columns": df.select_dtypes(include=['datetime']).columns.tolist()
        }
        return info
    except Exception as e:
        logger.log_etl_step("ERROR", f"Failed to get dataframe info: {str(e)}")
        return {}

def display_dataframe_info(df: pd.DataFrame, title: str = "Dataset Information", show_column_details: bool = True):
    """Display dataframe information in a nice format"""
    if title:
        st.subheader(title)
    
    info = get_dataframe_info(df)
    if not info:
        st.error("Unable to retrieve dataset information")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Rows", f"{info['rows']:,}")
    
    with col2:
        st.metric("Total Columns", info['columns'])
    
    with col3:
        st.metric("Memory Usage", f"{info['memory_usage']:.2f} MB")
    
    with col4:
        st.metric("Duplicate Rows", info['duplicate_rows'])
    
    # Column type breakdown (optional)
    if show_column_details:
        with st.expander("Column Details"):
            col_types = pd.DataFrame({
                "Column": df.columns,
                "Data Type": [str(dtype) for dtype in df.dtypes.values],
                "Null Count": df.isnull().sum().values,
                "Null %": (df.isnull().sum() / len(df) * 100).round(2).values
            })
            st.dataframe(col_types, use_container_width=True)

def validate_file_upload(uploaded_file) -> Tuple[bool, str]:
    """Validate uploaded file"""
    if uploaded_file is None:
        return False, "No file uploaded"
    
    file_size_mb = uploaded_file.size / (1024 * 1024)
    if file_size_mb > 100:  # 100MB limit
        return False, f"File too large: {file_size_mb:.2f}MB (max 100MB)"
    
    allowed_extensions = ['.csv', '.xlsx', '.xls', '.json']
    file_extension = f".{uploaded_file.name.split('.')[-1].lower()}"
    
    if file_extension not in allowed_extensions:
        return False, f"Unsupported file type: {file_extension}"
    
    return True, "File validation passed"

def create_progress_tracker(steps: List[str]) -> Dict[str, Any]:
    """Create a progress tracker for ETL steps"""
    return {
        "steps": steps,
        "current_step": 0,
        "completed_steps": [],
        "start_time": datetime.now(),
        "step_times": {}
    }

def update_progress_tracker(tracker: Optional[Dict[str, Any]], step: str, completed: bool = True):
    """Update progress tracker"""
    if tracker is None:
        return  # Skip update if tracker is None
    
    if completed and step not in tracker["completed_steps"]:
        tracker["completed_steps"].append(step)
        tracker["step_times"][step] = datetime.now()
        
        if step in tracker["steps"]:
            tracker["current_step"] = tracker["steps"].index(step) + 1

def display_progress_tracker(tracker: Optional[Dict[str, Any]]):
    """Display progress tracker"""
    if not tracker:
        return
    
    progress = len(tracker["completed_steps"]) / len(tracker["steps"])
    st.progress(progress)
    
    st.write(f"Progress: {len(tracker['completed_steps'])}/{len(tracker['steps'])} steps completed")
    
    if tracker["completed_steps"]:
        st.write("✅ Completed steps:", ", ".join(tracker["completed_steps"]))

def format_number(num: float, decimal_places: int = 2) -> str:
    """Format number with thousand separators"""
    return f"{num:,.{decimal_places}f}"

def safe_json_loads(json_str: str) -> Optional[Dict]:
    """Safely load JSON string"""
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return None

def get_column_suggestions(df: pd.DataFrame, operation: str) -> List[str]:
    """Get column suggestions based on operation type"""
    if operation == "numeric":
        return df.select_dtypes(include=['number']).columns.tolist()
    elif operation == "categorical":
        return df.select_dtypes(include=['object']).columns.tolist()
    elif operation == "datetime":
        return df.select_dtypes(include=['datetime']).columns.tolist()
    elif operation == "all":
        return df.columns.tolist()
    else:
        return []

def create_download_link(data, filename: str, file_format: str = "csv"):
    """Create download link for processed data"""
    if file_format.lower() == "csv":
        csv_data = data.to_csv(index=False)
        st.download_button(
            label=f"📥 Download {filename}.csv",
            data=csv_data,
            file_name=f"{filename}.csv",
            mime="text/csv"
        )
    elif file_format.lower() in ["xlsx", "excel"]:
        # For Excel files, we'll use a temporary approach
        st.info("💡 Excel download: Use the 'Export to Excel' button in the Load section")

def initialize_session_state():
    """Initialize session state variables"""
    default_values = {
        "extracted_data": None,
        "transformed_data": None,
        "current_data": None,
        "etl_history": [],
        "progress_tracker": None,
        "selected_columns": [],
        "transformation_log": [],
        "current_page": "home",
        "column_typo_suggestions": {},
        "data_typo_suggestions": {}
    }
    
    for key, value in default_values.items():
        if key not in st.session_state:
            st.session_state[key] = value