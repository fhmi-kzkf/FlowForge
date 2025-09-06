"""
Load Data page for FlowForge ETL Platform
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any

from modules.load import loader
from utils.helpers import (
    display_success_message, display_error_message, display_info_message,
    display_dataframe_info, format_number, update_progress_tracker
)
from utils.logger import logger

def show_load_page():
    """Display the data loading/export page"""
    
    st.title("💾 Load Data")
    st.markdown("Save your processed data in various formats")
    
    # Check if data is available
    if st.session_state.get("current_data") is None:
        st.warning("⚠️ No data available for loading. Please extract and transform data first.")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📂 Go to Extract"):
                logger.log_user_action("Navigate to Extract from Load (no data)")
                st.session_state.current_page = "extract"
                st.rerun()
        with col2:
            if st.button("🔍 Go to Transform"):
                logger.log_user_action("Navigate to Transform from Load (no data)")
                st.session_state.current_page = "transform"
                st.rerun()
        return
    
    df = st.session_state.current_data
    
    # Update progress tracker
    if st.session_state.get("progress_tracker"):
        update_progress_tracker(st.session_state.progress_tracker, "Load")
    
    # Show data summary
    st.header("📊 Data Export Summary")
    summary = loader.get_export_summary(df)
    
    if "error" in summary:
        display_error_message(summary["error"])
        return
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Rows", f"{summary['total_rows']:,}")
    
    with col2:
        st.metric("Total Columns", summary['total_columns'])
    
    with col3:
        st.metric("Memory Usage", f"{summary['memory_usage_mb']:.2f} MB")
    
    with col4:
        st.metric("Estimated CSV Size", f"{summary['estimated_csv_size_mb']:.2f} MB")
    
    # Column type breakdown
    with st.expander("📋 Column Type Breakdown"):
        col_type_col1, col_type_col2, col_type_col3 = st.columns(3)
        
        with col_type_col1:
            st.metric("Numeric Columns", summary['numeric_columns'])
        with col_type_col2:
            st.metric("Text Columns", summary['text_columns'])
        with col_type_col3:
            st.metric("Null Values", summary['null_values'])
    
    # Export tabs
    st.markdown("---")
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📥 Quick Download",
        "📁 Save to File", 
        "🗃️ Save to Database",
        "📋 Export History",
        "🔍 Data Preview"
    ])
    
    # Tab 1: Quick Download
    with tab1:
        st.header("📥 Quick Download")
        st.markdown("Download your data directly to your browser")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            download_format = st.selectbox(
                "Select download format:",
                options=["csv", "xlsx", "json"],
                format_func=lambda x: {
                    "csv": "📊 CSV (Comma Separated Values)",
                    "xlsx": "📈 Excel Workbook (.xlsx)",
                    "json": "🔗 JSON (JavaScript Object Notation)"
                }[x]
            )
        
        with col2:
            include_index = st.checkbox("Include row index", value=False)
        
        # Format-specific options
        format_options = {}
        
        if download_format == "xlsx":
            sheet_name = st.text_input("Excel sheet name:", value="FlowForge_Data")
            format_options["sheet_name"] = sheet_name
        elif download_format == "json":
            json_orient = st.selectbox(
                "JSON orientation:",
                options=["records", "index", "values", "columns"],
                help="How to structure the JSON output"
            )
            format_options["orient"] = json_orient
        
        format_options["include_index"] = include_index
        
        # Create download button
        data_bytes, mime_type, filename = loader.create_download_data(df, download_format, **format_options)
        
        if data_bytes:
            st.download_button(
                label=f"📥 Download as {download_format.upper()}",
                data=data_bytes,
                file_name=filename,
                mime=mime_type,
                type="primary",
                use_container_width=True
            )
            
            # Show download info
            st.success(f"✅ Ready to download: {filename} ({len(data_bytes) / (1024*1024):.2f} MB)")
        else:
            display_error_message(filename)  # Error message is in filename when data_bytes is None
    
    # Tab 2: Save to File
    with tab2:
        st.header("📁 Save to File")
        st.markdown("Save data to the server's file system")
        
        # File naming
        col1, col2 = st.columns([2, 1])
        
        with col1:
            filename = st.text_input(
                "File name (without extension):",
                value="flowforge_export",
                help="File will be saved in the exports directory"
            )
        
        with col2:
            file_format = st.selectbox(
                "File format:",
                options=["csv", "xlsx", "json"],
                format_func=lambda x: x.upper()
            )
        
        # Format-specific settings
        if file_format == "csv":
            include_index_csv = st.checkbox("Include row index in CSV", value=False)
            
            if st.button("💾 Save as CSV", type="primary"):
                success, message, file_path = loader.save_to_csv(df, filename, include_index_csv)
                if success:
                    display_success_message(message)
                    logger.log_user_action(f"Saved data as CSV: {filename}")
                else:
                    display_error_message(message)
        
        elif file_format == "xlsx":
            col_xlsx1, col_xlsx2 = st.columns(2)
            with col_xlsx1:
                sheet_name_xlsx = st.text_input("Sheet name:", value="Sheet1")
            with col_xlsx2:
                include_index_xlsx = st.checkbox("Include row index in Excel", value=False)
            
            if st.button("💾 Save as Excel", type="primary"):
                success, message, file_path = loader.save_to_excel(df, filename, sheet_name_xlsx, include_index_xlsx)
                if success:
                    display_success_message(message)
                    logger.log_user_action(f"Saved data as Excel: {filename}")
                else:
                    display_error_message(message)
        
        elif file_format == "json":
            json_orient_save = st.selectbox(
                "JSON orientation:",
                options=["records", "index", "values", "columns"],
                help="How to structure the JSON output",
                key="save_json_orient"
            )
            
            if st.button("💾 Save as JSON", type="primary"):
                success, message, file_path = loader.save_to_json(df, filename, json_orient_save)
                if success:
                    display_success_message(message)
                    logger.log_user_action(f"Saved data as JSON: {filename}")
                else:
                    display_error_message(message)
    
    # Tab 3: Save to Database
    with tab3:
        st.header("🗃️ Save to SQLite Database")
        st.markdown("Store your data in a SQLite database")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            table_name = st.text_input(
                "Table name:",
                value="flowforge_table",
                help="Name of the table in the database"
            )
        
        with col2:
            if_exists_action = st.selectbox(
                "If table exists:",
                options=["replace", "append", "fail"],
                format_func=lambda x: {
                    "replace": "Replace existing table",
                    "append": "Append to existing table",
                    "fail": "Fail if table exists"
                }[x]
            )
        
        # Database options
        use_default_db = st.checkbox("Use default FlowForge database", value=True)
        
        if not use_default_db:
            custom_db_path = st.text_input(
                "Custom database path:",
                placeholder="/path/to/database.db"
            )
            db_path = custom_db_path if custom_db_path else None
        else:
            db_path = None
            st.info("📝 Will use default database: flowforge_data.db in exports folder")
        
        if st.button("🗃️ Save to Database", type="primary"):
            success, message = loader.save_to_sqlite(df, table_name, db_path, if_exists_action)
            if success:
                display_success_message(message)
                logger.log_user_action(f"Saved data to SQLite table: {table_name}")
            else:
                display_error_message(message)
    
    # Tab 4: Export History
    with tab4:
        st.header("📋 Export History")
        
        # Show recent exports from current session
        export_history = loader.get_export_history()
        
        if export_history:
            st.subheader("🕒 Current Session Exports")
            
            for i, export in enumerate(reversed(export_history)):
                with st.container():
                    col1, col2, col3 = st.columns([2, 2, 1])
                    
                    with col1:
                        st.write(f"**{export['operation']}**")
                        st.write(f"📁 {export['destination']}")
                    
                    with col2:
                        st.write(f"📊 {export['rows']:,} rows × {export['columns']} cols")
                        if export.get('file_size_mb'):
                            st.write(f"💾 {export['file_size_mb']:.2f} MB")
                    
                    with col3:
                        st.write(f"🕐 {export['timestamp']}")
                    
                    if i < len(export_history) - 1:
                        st.markdown("---")
        else:
            st.info("No exports in current session.")
        
        st.markdown("---")
        
        # Show saved files
        st.subheader("💾 Saved Files")
        saved_files = loader.list_saved_files()
        
        for file_type, files in saved_files.items():
            if files:
                st.write(f"**{file_type.upper()} Files:**")
                
                for file_info in files:
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        st.write(f"📄 {file_info['name']}")
                    
                    with col2:
                        st.write(f"💾 {file_info['size_mb']:.2f} MB")
                    
                    with col3:
                        st.write(f"🕐 {file_info['modified']}")
                
                st.markdown("---")
        
        if not any(saved_files.values()):
            st.info("No saved files found in exports directory.")
    
    # Tab 5: Data Preview
    with tab5:
        st.header("🔍 Final Data Preview")
        st.markdown("Review your data before export")
        
        # Display comprehensive data info
        display_dataframe_info(df, "📊 Final Dataset Information")
        
        # Data preview options
        col1, col2 = st.columns([1, 1])
        with col1:
            preview_rows = st.slider("Rows to display:", 5, min(100, len(df)), 20)
        with col2:
            show_all_columns_load = st.checkbox("Show all columns", value=False, key="load_preview")
        
        # Show data
        if show_all_columns_load or len(df.columns) <= 15:
            st.dataframe(df.head(preview_rows), use_container_width=True)
        else:
            st.dataframe(df.iloc[:, :15].head(preview_rows), use_container_width=True)
            st.info(f"Showing first 15 of {len(df.columns)} columns")
        
        # Data quality summary
        with st.expander("🔍 Data Quality Summary"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Missing Values by Column:**")
                missing_data = df.isnull().sum()
                missing_df = pd.DataFrame({
                    'Column': missing_data.index,
                    'Missing Count': missing_data.values,
                    'Missing %': (missing_data.values / len(df) * 100).round(2)
                })
                st.dataframe(missing_df[missing_df['Missing Count'] > 0], use_container_width=True)
            
            with col2:
                st.write("**Data Types:**")
                dtype_counts = df.dtypes.value_counts()
                dtype_df = pd.DataFrame({
                    'Data Type': [str(dtype) for dtype in dtype_counts.index],
                    'Count': dtype_counts.values
                })
                st.dataframe(dtype_df, use_container_width=True)
    
    # Navigation and completion
    st.markdown("---")
    
    # Mark ETL as complete if data has been processed
    if st.session_state.get("progress_tracker"):
        update_progress_tracker(st.session_state.progress_tracker, "Complete")
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("⬅️ Back to Transform", use_container_width=True):
            logger.log_user_action("Navigate back to Transform from Load")
            st.session_state.current_page = "transform"
            st.rerun()
    
    with col2:
        if st.button("📊 View Dashboard", use_container_width=True):
            logger.log_user_action("Navigate to Dashboard from Load")
            st.session_state.current_page = "dashboard"
            st.rerun()
    
    with col3:
        if st.button("🔄 Start New Pipeline", use_container_width=True):
            # Clear current data and start fresh
            st.session_state.current_data = None
            st.session_state.extracted_data = None
            st.session_state.transformation_log = []
            logger.log_user_action("Started new pipeline from Load")
            st.session_state.current_page = "extract"
            st.rerun()
    
    # Success message for completed pipeline
    if st.session_state.get("progress_tracker") and \
       len(st.session_state.progress_tracker.get("completed_steps", [])) >= 3:
        st.success("🎉 **ETL Pipeline Complete!** Your data has been successfully extracted, transformed, and is ready for loading.")
        
        # Show final statistics
        with st.expander("📈 Pipeline Summary"):
            tracker = st.session_state.progress_tracker
            
            if tracker.get("step_times"):
                st.write("**Step Completion Times:**")
                for step, time in tracker["step_times"].items():
                    st.write(f"✅ {step}: {time}")
            
            if st.session_state.get("transformation_log"):
                st.write(f"**Total Transformations Applied:** {len(st.session_state.transformation_log)}")
            
            st.write(f"**Final Dataset:** {len(df):,} rows × {len(df.columns)} columns")
            st.write(f"**Memory Usage:** {summary['memory_usage_mb']:.2f} MB")