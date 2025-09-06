"""
Transform Data page for FlowForge ETL Platform
"""

import streamlit as st
import pandas as pd
import numpy as np
from typing import List, Dict

from modules.transform import transformer
from utils.helpers import (
    display_success_message, display_error_message, display_info_message,
    display_dataframe_info, get_column_suggestions, update_progress_tracker
)
from utils.logger import logger

def show_transform_page():
    """Display the data transformation page"""
    
    st.title("🔍 Transform Data")
    st.markdown("Clean, filter, and transform your data for analysis")
    
    # Check if data is available
    if st.session_state.get("current_data") is None:
        st.warning("⚠️ No data available for transformation. Please extract data first.")
        if st.button("📂 Go to Extract"):
            logger.log_user_action("Navigate to Extract from Transform (no data)")
            st.session_state.current_page = "extract"
            st.rerun()
        return
    
    df = st.session_state.current_data.copy()
    
    # Update progress tracker
    if st.session_state.get("progress_tracker"):
        update_progress_tracker(st.session_state.progress_tracker, "Transform")
    
    # Show current data info
    with st.expander("📊 Current Dataset Overview", expanded=True):
        display_dataframe_info(df, title="", show_column_details=False)
        
        # Add column details here directly to avoid nested expanders
        col_types = pd.DataFrame({
            "Column": df.columns,
            "Data Type": [str(dtype) for dtype in df.dtypes.values],
            "Null Count": df.isnull().sum().values,
            "Null %": (df.isnull().sum() / len(df) * 100).round(2).values
        })
        
        st.markdown("**Column Details:**")
        st.dataframe(col_types, use_container_width=True)
    
    # Transformation tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "🧹 Data Cleaning",
        "🔍 Filter & Sort", 
        "📝 Column Operations",
        "🔧 Data Types",
        "📊 Calculations",
        "🔤 Text Operations",
        "✨ Fix Typos"
    ])
    
    # Initialize transformation log in session state
    if "transformation_log" not in st.session_state:
        st.session_state.transformation_log = []
    
    # Tab 1: Data Cleaning
    with tab1:
        st.header("🧹 Data Cleaning")
        
        # Remove duplicates section
        st.subheader("🔄 Remove Duplicates")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            duplicate_columns = st.multiselect(
                "Select columns to check for duplicates (leave empty for all):",
                options=df.columns.tolist(),
                help="Choose specific columns or leave empty to check entire rows"
            )
        
        with col2:
            keep_option = st.selectbox(
                "Which duplicate to keep:",
                options=["first", "last"],
                format_func=lambda x: {"first": "Keep First", "last": "Keep Last"}[x]
            )
        
        duplicate_count = df.duplicated(subset=duplicate_columns if duplicate_columns else None).sum()
        st.info(f"Found {duplicate_count} duplicate rows")
        
        if st.button("🗑️ Remove Duplicates", key="remove_dupes") and duplicate_count > 0:
            df, message = transformer.remove_duplicates(
                df, columns=duplicate_columns if duplicate_columns else None, keep=keep_option
            )
            st.session_state.current_data = df
            display_success_message(message)
            st.session_state.transformation_log.append(message)
            st.rerun()
        
        st.markdown("---")
        
        # Handle missing values section
        st.subheader("❓ Handle Missing Values")
        
        # Show missing values overview
        missing_counts = df.isnull().sum()
        missing_cols = missing_counts[missing_counts > 0]
        
        if len(missing_cols) > 0:
            st.warning(f"Found missing values in {len(missing_cols)} columns:")
            
            col1, col2 = st.columns(2)
            with col1:
                st.dataframe(
                    pd.DataFrame({
                        'Column': missing_cols.index,
                        'Missing Count': missing_cols.values,
                        'Missing %': (missing_cols.values / len(df) * 100).round(2)
                    }),
                    use_container_width=True
                )
            
            with col2:
                # Missing values handling options
                missing_columns = st.multiselect(
                    "Select columns to handle:",
                    options=missing_cols.index.tolist(),
                    default=missing_cols.index.tolist()[:3] if len(missing_cols) <= 3 else missing_cols.index.tolist()[:3]
                )
                
                missing_method = st.selectbox(
                    "Method to handle missing values:",
                    options=[
                        "drop", "fill_mean", "fill_median", "fill_mode", 
                        "fill_value", "forward_fill", "backward_fill"
                    ],
                    format_func=lambda x: {
                        "drop": "Drop rows with missing values",
                        "fill_mean": "Fill with mean (numeric only)",
                        "fill_median": "Fill with median (numeric only)",
                        "fill_mode": "Fill with most frequent value",
                        "fill_value": "Fill with custom value",
                        "forward_fill": "Forward fill (use previous value)",
                        "backward_fill": "Backward fill (use next value)"
                    }[x]
                )
                
                fill_value = None
                if missing_method == "fill_value":
                    fill_value = st.text_input("Custom fill value:", value="Unknown")
                
                if st.button("🔧 Handle Missing Values", key="handle_missing"):
                    df, message = transformer.handle_missing_values(
                        df, method=missing_method, columns=missing_columns, fill_value=fill_value
                    )
                    st.session_state.current_data = df
                    display_success_message(message)
                    st.session_state.transformation_log.append(message)
                    st.rerun()
        else:
            st.success("✅ No missing values found in the dataset!")
    
    # Tab 2: Filter & Sort
    with tab2:
        st.header("🔍 Filter & Sort Data")
        
        # Filter section
        st.subheader("🎯 Filter Data")
        col1, col2, col3 = st.columns([2, 1, 2])
        
        with col1:
            filter_column = st.selectbox(
                "Select column to filter:",
                options=df.columns.tolist(),
                key="filter_column"
            )
        
        with col2:
            filter_operator = st.selectbox(
                "Operator:",
                options=["==", "!=", ">", "<", ">=", "<=", "contains", "startswith", "endswith"],
                format_func=lambda x: {
                    "==": "Equals (==)",
                    "!=": "Not equals (!=)",
                    ">": "Greater than (>)",
                    "<": "Less than (<)",
                    ">=": "Greater or equal (>=)",
                    "<=": "Less or equal (<=)",
                    "contains": "Contains text",
                    "startswith": "Starts with",
                    "endswith": "Ends with"
                }[x]
            )
        
        with col3:
            if filter_column in df.select_dtypes(include=[np.number]).columns:
                filter_value = st.number_input("Filter value:", value=0.0)
            else:
                filter_value = st.text_input("Filter value:", value="")
        
        if st.button("🎯 Apply Filter", key="apply_filter"):
            df, message = transformer.filter_data(df, filter_column, filter_operator, filter_value)
            st.session_state.current_data = df
            display_success_message(message)
            st.session_state.transformation_log.append(message)
            st.rerun()
        
        st.markdown("---")
        
        # Sort section
        st.subheader("📊 Sort Data")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            sort_columns = st.multiselect(
                "Select columns to sort by (in order of priority):",
                options=df.columns.tolist(),
                max_selections=3
            )
        
        with col2:
            if sort_columns:
                sort_orders = []
                for i, col in enumerate(sort_columns):
                    order = st.checkbox(f"Ascending ({col})", value=True, key=f"sort_asc_{i}")
                    sort_orders.append(order)
        
        if sort_columns and st.button("📊 Apply Sort", key="apply_sort"):
            df, message = transformer.sort_data(df, sort_columns, sort_orders)
            st.session_state.current_data = df
            display_success_message(message)
            st.session_state.transformation_log.append(message)
            st.rerun()
    
    # Tab 3: Column Operations
    with tab3:
        st.header("📝 Column Operations")
        
        # Rename columns
        st.subheader("✏️ Rename Columns")
        rename_mapping = {}
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Current Names:**")
            selected_rename_cols = st.multiselect(
                "Select columns to rename:",
                options=df.columns.tolist(),
                key="rename_cols"
            )
        
        with col2:
            st.markdown("**New Names:**")
            if selected_rename_cols:
                for col in selected_rename_cols:
                    new_name = st.text_input(f"New name for '{col}':", value=col, key=f"new_name_{col}")
                    if new_name != col:
                        rename_mapping[col] = new_name
        
        if rename_mapping and st.button("✏️ Rename Columns", key="rename_cols_btn"):
            df, message = transformer.rename_columns(df, rename_mapping)
            st.session_state.current_data = df
            display_success_message(message)
            st.session_state.transformation_log.append(message)
            st.rerun()
        
        st.markdown("---")
        
        # Drop columns
        st.subheader("🗑️ Drop Columns")
        drop_columns = st.multiselect(
            "Select columns to drop:",
            options=df.columns.tolist(),
            help="These columns will be permanently removed from the dataset"
        )
        
        if drop_columns:
            st.warning(f"⚠️ You are about to drop {len(drop_columns)} columns: {', '.join(drop_columns)}")
            
            if st.button("🗑️ Drop Selected Columns", key="drop_cols", type="secondary"):
                df, message = transformer.drop_columns(df, drop_columns)
                st.session_state.current_data = df
                display_success_message(message)
                st.session_state.transformation_log.append(message)
                st.rerun()
    
    # Tab 4: Data Types
    with tab4:
        st.header("🔧 Data Type Conversions")
        
        # Show current data types
        st.subheader("📋 Current Data Types")
        
        # Create sample values safely
        sample_values = []
        for col in df.columns:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                sample_values.append(str(col_data.iloc[0]))
            else:
                sample_values.append('N/A')
        
        type_df = pd.DataFrame({
            'Column': df.columns,
            'Current Type': [str(dtype) for dtype in df.dtypes.values],
            'Non-null Count': df.count().values,
            'Sample Values': sample_values
        })
        st.dataframe(type_df, use_container_width=True)
        
        # Type conversion
        st.subheader("🔄 Convert Data Types")
        
        type_mapping = {}
        conversion_cols = st.multiselect(
            "Select columns to convert:",
            options=df.columns.tolist()
        )
        
        if conversion_cols:
            for col in conversion_cols:
                target_type = st.selectbox(
                    f"Convert '{col}' to:",
                    options=["string", "int", "float", "datetime", "boolean", "category"],
                    key=f"type_{col}"
                )
                type_mapping[col] = target_type
        
        if type_mapping and st.button("🔄 Convert Types", key="convert_types"):
            df, message = transformer.convert_data_types(df, type_mapping)
            st.session_state.current_data = df
            display_success_message(message)
            st.session_state.transformation_log.append(message)
            st.rerun()
    
    # Tab 5: Calculations
    with tab5:
        st.header("📊 Create Calculated Columns")
        
        st.subheader("➕ Add New Calculated Column")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            new_column_name = st.text_input("New column name:", placeholder="calculated_column")
        
        with col2:
            # Show available columns
            st.markdown("**Available columns:**")
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            st.write("Numeric:", ", ".join(numeric_cols) if numeric_cols else "None")
            
        # Expression input
        st.markdown("**Expression:**")
        expression_help = """
        Examples:
        - Column1 + Column2
        - Column1 * 1.5
        - (Column1 + Column2) / 2
        - Column1.abs() (absolute value)
        - Column1.fillna(0) (fill missing with 0)
        """
        
        expression = st.text_area(
            "Python expression using column names:",
            placeholder="Column1 + Column2",
            help=expression_help
        )
        
        if new_column_name and expression and st.button("➕ Create Column", key="create_calc_col"):
            df, message = transformer.create_calculated_column(df, new_column_name, expression)
            st.session_state.current_data = df
            display_success_message(message)
            st.session_state.transformation_log.append(message)
            st.rerun()
    
    # Tab 6: Text Operations
    with tab6:
        st.header("🔤 Text Operations")
        
        # Get text columns
        text_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        if not text_cols:
            st.info("No text columns found in the dataset.")
        else:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                text_column = st.selectbox("Select text column:", options=text_cols)
                
                text_operation = st.selectbox(
                    "Select operation:",
                    options=["upper", "lower", "title", "strip", "replace", "extract"],
                    format_func=lambda x: {
                        "upper": "Convert to UPPERCASE",
                        "lower": "convert to lowercase",
                        "title": "Convert To Title Case",
                        "strip": "Remove leading/trailing spaces",
                        "replace": "Replace text",
                        "extract": "Extract pattern (regex)"
                    }[x]
                )
            
            with col2:
                # Additional parameters for specific operations
                operation_params = {}
                
                if text_operation == "replace":
                    operation_params["old_value"] = st.text_input("Text to replace:", key="replace_old")
                    operation_params["new_value"] = st.text_input("Replace with:", key="replace_new")
                elif text_operation == "extract":
                    operation_params["pattern"] = st.text_input(
                        "Regex pattern:", 
                        placeholder=r"(\d+)",
                        help="Use parentheses to capture groups",
                        key="extract_pattern"
                    )
                
                # Show sample values
                if text_column:
                    st.markdown("**Sample values:**")
                    sample_values = df[text_column].dropna().head(3).tolist()
                    for val in sample_values:
                        st.code(str(val))
            
            if st.button("🔤 Apply Text Operation", key="apply_text_op"):
                df, message = transformer.text_operations(
                    df, text_column, text_operation, **operation_params
                )
                st.session_state.current_data = df
                display_success_message(message)
                st.session_state.transformation_log.append(message)
                st.rerun()
    
    # Tab 7: Fix Typos
    with tab7:
        st.header("✨ Fix Typos")
        st.markdown("Detect and fix typos in column names and data values")
        
        # Column typos section
        st.subheader("🏷️ Column Name Typos")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Current Column Names:**")
            st.dataframe(pd.DataFrame({"Columns": df.columns.tolist()}), use_container_width=True)
            
            # Get suggestions for column names
            if st.button("🔍 Detect Column Typos", key="detect_col_typos"):
                suggestions = transformer.suggest_typo_fixes(df)
                if suggestions["column_suggestions"]:
                    st.session_state.column_typo_suggestions = suggestions["column_suggestions"]
                    st.rerun()
                else:
                    st.success("No obvious column name typos detected!")
        
        with col2:
            st.markdown("**Fix Column Names:**")
            
            # Manual column renaming
            selected_col_to_fix = st.selectbox(
                "Select column to fix:",
                options=[""] + df.columns.tolist(),
                key="col_to_fix"
            )
            
            if selected_col_to_fix:
                new_col_name = st.text_input(
                    f"New name for '{selected_col_to_fix}':",
                    value=selected_col_to_fix,
                    key="new_col_name"
                )
                
                if new_col_name != selected_col_to_fix and st.button("✏️ Fix Column Name", key="fix_col_name"):
                    df, message = transformer.fix_column_typos(df, {selected_col_to_fix: new_col_name})
                    st.session_state.current_data = df
                    display_success_message(message)
                    st.session_state.transformation_log.append(message)
                    st.rerun()
        
        # Show automatic suggestions if available
        if st.session_state.get("column_typo_suggestions"):
            st.markdown("**🤖 Suggested Column Fixes:**")
            suggestions = st.session_state.column_typo_suggestions
            
            col_fixes = {}
            for col, matches in suggestions.items():
                suggested_fix = st.selectbox(
                    f"Fix '{col}' to:",
                    options=["Keep as is"] + matches,
                    key=f"suggest_col_{col}"
                )
                if suggested_fix != "Keep as is":
                    col_fixes[col] = suggested_fix
            
            if col_fixes and st.button("🎯 Apply Suggested Fixes", key="apply_col_fixes"):
                df, message = transformer.fix_column_typos(df, col_fixes)
                st.session_state.current_data = df
                display_success_message(message)
                st.session_state.transformation_log.append(message)
                st.session_state.column_typo_suggestions = {}
                st.rerun()
        
        st.markdown("---")
        
        # Data value typos section
        st.subheader("📊 Data Value Typos")
        
        # Get text columns for typo detection
        text_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        if not text_cols:
            st.info("No text columns found for typo detection.")
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                selected_data_col = st.selectbox(
                    "Select column to check for data typos:",
                    options=text_cols,
                    key="data_typo_col"
                )
                
                if selected_data_col and st.button("🔍 Detect Data Typos", key="detect_data_typos"):
                    suggestions = transformer.suggest_typo_fixes(df, selected_data_col)
                    if suggestions["data_suggestions"]:
                        st.session_state.data_typo_suggestions = {
                            "column": selected_data_col,
                            "suggestions": suggestions["data_suggestions"]
                        }
                        st.rerun()
                    else:
                        st.success("No obvious data typos detected!")
                
                # Show value counts for selected column
                if selected_data_col:
                    st.markdown("**Value Distribution:**")
                    value_counts = df[selected_data_col].value_counts().head(10)
                    st.dataframe(value_counts.reset_index(), use_container_width=True)
            
            with col2:
                st.markdown("**Manual Data Fixes:**")
                
                if selected_data_col:
                    # Manual value replacement
                    old_value = st.text_input("Value to replace:", key="old_data_value")
                    new_value = st.text_input("Replace with:", key="new_data_value")
                    
                    if old_value and new_value and st.button("✏️ Fix Data Value", key="fix_data_value"):
                        df, message = transformer.fix_data_typos(df, selected_data_col, {old_value: new_value})
                        st.session_state.current_data = df
                        display_success_message(message)
                        st.session_state.transformation_log.append(message)
                        st.rerun()
            
            # Show automatic suggestions for data values
            if st.session_state.get("data_typo_suggestions"):
                suggestions_data = st.session_state.data_typo_suggestions
                if suggestions_data["column"] == selected_data_col:
                    st.markdown("**🤖 Suggested Data Fixes:**")
                    
                    data_fixes = {}
                    for value, matches in suggestions_data["suggestions"].items():
                        suggested_fix = st.selectbox(
                            f"Fix '{value}' to:",
                            options=["Keep as is"] + matches,
                            key=f"suggest_data_{value}"
                        )
                        if suggested_fix != "Keep as is":
                            data_fixes[value] = suggested_fix
                    
                    if data_fixes and st.button("🎯 Apply Data Fixes", key="apply_data_fixes"):
                        df, message = transformer.fix_data_typos(df, selected_data_col, data_fixes)
                        st.session_state.current_data = df
                        display_success_message(message)
                        st.session_state.transformation_log.append(message)
                        st.session_state.data_typo_suggestions = {}
                        st.rerun()
    
    # Show transformation log
    st.markdown("---")
    st.header("📋 Transformation History")
    
    if st.session_state.transformation_log:
        for i, log_entry in enumerate(reversed(st.session_state.transformation_log)):
            st.text(f"{len(st.session_state.transformation_log) - i}. {log_entry}")
    else:
        st.info("No transformations applied yet.")
    
    # Current data preview
    st.markdown("---")
    st.header("📊 Transformed Data Preview")
    
    # Show updated data info
    display_dataframe_info(df, "")
    
    # Data preview
    col1, col2 = st.columns([1, 1])
    with col1:
        preview_rows = st.slider("Rows to display:", 5, min(50, len(df)), 10)
    with col2:
        show_all_columns = st.checkbox("Show all columns", value=False, key="transform_preview")
    
    if show_all_columns or len(df.columns) <= 10:
        st.dataframe(df.head(preview_rows), use_container_width=True)
    else:
        st.dataframe(df.iloc[:, :10].head(preview_rows), use_container_width=True)
        st.info(f"Showing first 10 of {len(df.columns)} columns")
    
    # Navigation buttons
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("⬅️ Back to Extract", use_container_width=True):
            logger.log_user_action("Navigate back to Extract from Transform")
            st.session_state.current_page = "extract"
            st.rerun()
    
    with col3:
        if st.button("➡️ Continue to Load", type="primary", use_container_width=True):
            logger.log_user_action("Navigate to Load from Transform")
            st.session_state.current_page = "load"
            st.rerun()