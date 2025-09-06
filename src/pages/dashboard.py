"""
Dashboard page for FlowForge ETL Platform
Provides monitoring, visualization, and pipeline overview
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from modules.load import loader
from modules.transform import transformer
from utils.helpers import (
    display_success_message, display_info_message, 
    get_dataframe_info, format_number
)
from utils.logger import logger
from utils.config import LOGS_DIR

def create_data_overview_charts(df: pd.DataFrame):
    """Create overview charts for the dataset"""
    
    # Data types distribution
    dtype_counts = df.dtypes.value_counts()
    fig_dtypes = px.pie(
        values=dtype_counts.values,
        names=[str(dtype) for dtype in dtype_counts.index],
        title="Data Types Distribution",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig_dtypes.update_layout(height=400)
    
    # Missing values heatmap
    missing_data = df.isnull().sum()
    if missing_data.sum() > 0:
        fig_missing = px.bar(
            x=missing_data.index,
            y=missing_data.values,
            title="Missing Values by Column",
            labels={'x': 'Columns', 'y': 'Missing Count'},
            color=missing_data.values,
            color_continuous_scale='Reds'
        )
        fig_missing.update_layout(height=400, xaxis_tickangle=-45)
    else:
        fig_missing = None
    
    # Numeric columns distribution
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        # Create distribution plots for up to 4 numeric columns
        cols_to_plot = numeric_cols[:4]
        fig_distributions = make_subplots(
            rows=2, cols=2,
            subplot_titles=cols_to_plot,
            vertical_spacing=0.12
        )
        
        for i, col in enumerate(cols_to_plot):
            row = (i // 2) + 1
            col_num = (i % 2) + 1
            
            fig_distributions.add_trace(
                go.Histogram(x=df[col], name=col, showlegend=False),
                row=row, col=col_num
            )
        
        fig_distributions.update_layout(height=500, title_text="Numeric Columns Distribution")
    else:
        fig_distributions = None
    
    return fig_dtypes, fig_missing, fig_distributions

def show_etl_pipeline_status():
    """Show ETL pipeline status and progress"""
    
    tracker = st.session_state.get("progress_tracker")
    
    if tracker:
        st.subheader("🔄 ETL Pipeline Status")
        
        # Progress visualization
        progress = len(tracker["completed_steps"]) / len(tracker["steps"])
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.progress(progress)
            st.write(f"**Progress:** {len(tracker['completed_steps'])}/{len(tracker['steps'])} steps completed")
        
        with col2:
            if progress == 1.0:
                st.success("✅ Complete")
            elif progress > 0.5:
                st.warning("🔄 In Progress")
            else:
                st.info("🔵 Starting")
        
        with col3:
            if tracker.get("start_time"):
                elapsed = datetime.now() - tracker["start_time"]
                st.metric("Elapsed Time", f"{elapsed.seconds // 60}m {elapsed.seconds % 60}s")
        
        # Step details
        st.write("**Pipeline Steps:**")
        for i, step in enumerate(tracker["steps"]):
            if step in tracker["completed_steps"]:
                st.write(f"✅ {i+1}. {step}")
            elif i == len(tracker["completed_steps"]):
                st.write(f"🔄 {i+1}. {step} (current)")
            else:
                st.write(f"⏳ {i+1}. {step}")
        
        # Step completion times
        if tracker.get("step_times"):
            st.write("**Completion Times:**")
            for step, completion_time in tracker["step_times"].items():
                st.write(f"• {step}: {completion_time}")
    
    else:
        st.info("No active ETL pipeline. Start by extracting data!")

def show_transformation_history():
    """Show transformation history and operations log"""
    
    st.subheader("📝 Transformation History")
    
    # Session transformation log
    if st.session_state.get("transformation_log"):
        st.write("**Current Session Transformations:**")
        for i, log_entry in enumerate(st.session_state.transformation_log, 1):
            st.write(f"{i}. {log_entry}")
        
        st.markdown("---")
    
    # Transformer history (detailed)
    history = transformer.get_transformation_history()
    
    if history:
        st.write("**Detailed Transformation Log:**")
        
        # Create a DataFrame for better display
        history_df = pd.DataFrame(history)
        
        # Display as expandable sections
        for i, record in enumerate(history):
            with st.expander(f"🔧 {record['operation']} - {record['timestamp']}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Operation:** {record['operation']}")
                    st.write(f"**Details:** {record['details']}")
                    st.write(f"**Timestamp:** {record['timestamp']}")
                
                with col2:
                    st.write(f"**Rows:** {record['rows_before']} → {record['rows_after']} ({record['rows_changed']:+d})")
                    st.write(f"**Columns:** {record['cols_before']} → {record['cols_after']} ({record['cols_changed']:+d})")
        
        # Summary statistics
        st.write("**Transformation Summary:**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Operations", len(history))
        with col2:
            total_row_changes = sum(abs(h['rows_changed']) for h in history)
            st.metric("Total Row Changes", total_row_changes)
        with col3:
            total_col_changes = sum(abs(h['cols_changed']) for h in history)
            st.metric("Total Column Changes", total_col_changes)
    
    else:
        st.info("No transformations recorded yet.")

def show_system_logs():
    """Show system logs and activity"""
    
    st.subheader("📊 System Activity")
    
    # Try to read the log file
    try:
        log_file = LOGS_DIR / "flowforge.log"
        
        if log_file.exists():
            # Read recent log entries
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Get last 50 lines
            recent_lines = lines[-50:] if len(lines) > 50 else lines
            
            if recent_lines:
                st.write(f"**Recent Activity** (last {len(recent_lines)} entries):")
                
                # Display in a scrollable container
                log_text = "".join(recent_lines)
                st.text_area("Activity Log", value=log_text, height=300)
                
                # Log statistics
                error_count = sum(1 for line in recent_lines if "ERROR" in line)
                warning_count = sum(1 for line in recent_lines if "WARNING" in line)
                info_count = len(recent_lines) - error_count - warning_count
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Info", info_count)
                with col2:
                    st.metric("Warnings", warning_count)
                with col3:
                    st.metric("Errors", error_count)
            else:
                st.info("No log entries found.")
        else:
            st.info("Log file not created yet. Start using the application to see activity logs.")
    
    except Exception as e:
        st.error(f"Error reading logs: {str(e)}")

def show_export_analytics():
    """Show export history and analytics"""
    
    st.subheader("📤 Export Analytics")
    
    export_history = loader.get_export_history()
    
    if export_history:
        # Create DataFrame for analysis
        exports_df = pd.DataFrame(export_history)
        
        # Export operations over time
        exports_df['timestamp'] = pd.to_datetime(exports_df['timestamp'])
        
        # Operations by type
        operation_counts = exports_df['operation'].value_counts()
        fig_operations = px.pie(
            values=operation_counts.values,
            names=operation_counts.index,
            title="Export Operations by Type"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(fig_operations, use_container_width=True)
        
        with col2:
            # Export metrics
            total_exports = len(exports_df)
            total_rows_exported = exports_df['rows'].sum()
            
            # Handle file size calculation safely
            if 'file_size_mb' in exports_df.columns:
                file_size_series = exports_df['file_size_mb'].dropna()
                avg_file_size = file_size_series.mean() if len(file_size_series) > 0 else 0
            else:
                avg_file_size = 0
            
            st.metric("Total Exports", total_exports)
            st.metric("Total Rows Exported", f"{total_rows_exported:,}")
            if avg_file_size > 0:
                st.metric("Avg File Size", f"{avg_file_size:.2f} MB")
        
        # Recent exports table
        st.write("**Recent Exports:**")
        display_exports = exports_df[['timestamp', 'operation', 'rows', 'columns', 'destination']].copy()
        display_exports['timestamp'] = display_exports['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        st.dataframe(display_exports, use_container_width=True)
    
    else:
        st.info("No exports recorded in this session.")

def show_data_quality_report(df: pd.DataFrame):
    """Show comprehensive data quality report"""
    
    st.subheader("🎯 Data Quality Report")
    
    # Basic quality metrics
    total_cells = len(df) * len(df.columns)
    null_cells = df.isnull().sum().sum()
    completeness = ((total_cells - null_cells) / total_cells) * 100
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Data Completeness", f"{completeness:.1f}%")
    
    with col2:
        duplicate_rows = df.duplicated().sum()
        st.metric("Duplicate Rows", duplicate_rows)
    
    with col3:
        unique_ratio = (df.nunique().sum() / len(df)) * 100
        st.metric("Avg Uniqueness", f"{unique_ratio:.1f}%")
    
    with col4:
        numeric_cols = len(df.select_dtypes(include=[np.number]).columns)
        st.metric("Numeric Columns", numeric_cols)
    
    # Detailed quality analysis
    with st.expander("📋 Detailed Quality Analysis"):
        quality_df = pd.DataFrame({
            'Column': df.columns,
            'Data Type': [str(dtype) for dtype in df.dtypes.values],
            'Non-Null Count': df.count(),
            'Null Count': df.isnull().sum(),
            'Null %': (df.isnull().sum() / len(df) * 100).round(2),
            'Unique Values': df.nunique(),
            'Uniqueness %': (df.nunique() / len(df) * 100).round(2)
        })
        
        st.dataframe(quality_df, use_container_width=True)
    
    # Quality score
    quality_scores = []
    
    # Completeness score (0-100)
    quality_scores.append(completeness)
    
    # Uniqueness score (penalize low uniqueness)
    avg_uniqueness = df.nunique().mean() / len(df) * 100
    quality_scores.append(min(avg_uniqueness * 2, 100))  # Scale appropriately
    
    # Consistency score (penalize too many data types)
    type_diversity = len(df.dtypes.value_counts()) / len(df.columns)
    consistency_score = max(0, 100 - (type_diversity * 50))
    quality_scores.append(consistency_score)
    
    overall_quality = np.mean(quality_scores)
    
    # Display quality score
    st.write("**Overall Data Quality Score:**")
    
    if overall_quality >= 80:
        st.success(f"🟢 Excellent: {overall_quality:.1f}/100")
    elif overall_quality >= 60:
        st.warning(f"🟡 Good: {overall_quality:.1f}/100")
    else:
        st.error(f"🔴 Needs Improvement: {overall_quality:.1f}/100")

def show_dashboard_page():
    """Display the main dashboard page"""
    
    st.title("📊 Dashboard & Monitoring")
    st.markdown("Monitor your ETL pipelines and analyze your data")
    
    # Check if there's any data to analyze
    has_current_data = st.session_state.get("current_data") is not None
    has_extracted_data = st.session_state.get("extracted_data") is not None
    
    # Dashboard tabs
    if has_current_data or has_extracted_data:
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📈 Data Overview",
            "🔄 Pipeline Status", 
            "📝 Activity Log",
            "📤 Export History",
            "🎯 Quality Report"
        ])
        
        # Use current data if available, otherwise use extracted data
        current_data = st.session_state.get("current_data")
        extracted_data = st.session_state.get("extracted_data")
        df = current_data if current_data is not None else extracted_data
        
        # Tab 1: Data Overview
        with tab1:
            st.header("📈 Data Overview")
            
            if df is not None:
                # Basic statistics
                info = get_dataframe_info(df)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Rows", f"{info['rows']:,}")
                
                with col2:
                    st.metric("Total Columns", info['columns'])
                
                with col3:
                    st.metric("Memory Usage", f"{info['memory_usage']:.2f} MB")
                
                with col4:
                    st.metric("Missing Values", info['null_counts'])
                
                # Create and display charts
                fig_dtypes, fig_missing, fig_distributions = create_data_overview_charts(df)
                
                # Display charts in columns
                col1, col2 = st.columns(2)
                
                with col1:
                    st.plotly_chart(fig_dtypes, use_container_width=True)
                
                with col2:
                    if fig_missing:
                        st.plotly_chart(fig_missing, use_container_width=True)
                    else:
                        st.success("🎉 No missing values detected!")
                
                # Distribution plots
                if fig_distributions:
                    st.plotly_chart(fig_distributions, use_container_width=True)
                
                # Sample data preview
                st.subheader("📋 Data Sample")
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    sample_size = st.slider("Sample size:", 5, min(50, len(df)), 10)
                with col2:
                    random_sample = st.checkbox("Random sample", value=False)
                
                if random_sample:
                    sample_df = df.sample(n=min(sample_size, len(df)))
                else:
                    sample_df = df.head(sample_size)
                
                st.dataframe(sample_df, use_container_width=True)
            
            else:
                st.info("No data available for overview.")
        
        # Tab 2: Pipeline Status
        with tab2:
            st.header("🔄 ETL Pipeline Monitoring")
            show_etl_pipeline_status()
            
            st.markdown("---")
            show_transformation_history()
        
        # Tab 3: Activity Log
        with tab3:
            st.header("📝 System Activity & Logs")
            show_system_logs()
        
        # Tab 4: Export History
        with tab4:
            st.header("📤 Export Analytics")
            show_export_analytics()
            
            st.markdown("---")
            
            # Saved files overview
            st.subheader("💾 Saved Files Overview")
            saved_files = loader.list_saved_files()
            
            total_files = sum(len(files) for files in saved_files.values())
            
            if total_files > 0:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("CSV Files", len(saved_files.get("csv", [])))
                with col2:
                    st.metric("Excel Files", len(saved_files.get("excel", [])))
                with col3:
                    st.metric("JSON Files", len(saved_files.get("json", [])))
                with col4:
                    st.metric("Database Files", len(saved_files.get("sqlite", [])))
                
                # File type distribution
                file_counts = {k: len(v) for k, v in saved_files.items() if v}
                if file_counts:
                    fig_files = px.bar(
                        x=list(file_counts.keys()),
                        y=list(file_counts.values()),
                        title="Saved Files by Type",
                        labels={'x': 'File Type', 'y': 'Count'}
                    )
                    st.plotly_chart(fig_files, use_container_width=True)
            else:
                st.info("No saved files found.")
        
        # Tab 5: Quality Report
        with tab5:
            st.header("🎯 Data Quality Analysis")
            
            if df is not None:
                show_data_quality_report(df)
                
                # Column-specific analysis
                st.markdown("---")
                st.subheader("🔍 Column Analysis")
                
                selected_column = st.selectbox(
                    "Select column for detailed analysis:",
                    options=df.columns.tolist()
                )
                
                if selected_column:
                    col_data = df[selected_column]
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Unique Values", col_data.nunique())
                    with col2:
                        st.metric("Missing Values", col_data.isnull().sum())
                    with col3:
                        if col_data.dtype in ['object']:
                            most_common = col_data.mode()
                            if not most_common.empty:
                                st.metric("Most Common", str(most_common.iloc[0])[:20])
                        else:
                            st.metric("Mean", f"{col_data.mean():.2f}")
                    
                    # Column visualization
                    if col_data.dtype in ['int64', 'float64']:
                        fig_col = px.histogram(df, x=selected_column, title=f"Distribution of {selected_column}")
                        st.plotly_chart(fig_col, use_container_width=True)
                    elif col_data.dtype == 'object':
                        value_counts = col_data.value_counts().head(10)
                        fig_col = px.bar(
                            x=value_counts.index,
                            y=value_counts.values,
                            title=f"Top 10 Values in {selected_column}"
                        )
                        st.plotly_chart(fig_col, use_container_width=True)
            else:
                st.info("No data available for quality analysis.")
    
    else:
        # No data available - show getting started
        st.info("📝 No data available for dashboard analysis.")
        
        st.markdown("### 🚀 Getting Started")
        st.markdown("""
        To see dashboard analytics and monitoring:
        
        1. **Extract Data** - Load data from files, APIs, or databases
        2. **Transform Data** - Clean and process your data  
        3. **Load Data** - Export your results
        
        The dashboard will then show:
        - 📊 Data visualizations and statistics
        - 🔄 ETL pipeline progress tracking
        - 📝 Transformation history and logs
        - 📤 Export analytics and file management
        - 🎯 Data quality assessments
        """)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📂 Start with Extract", type="primary", use_container_width=True):
                logger.log_user_action("Navigate to Extract from Dashboard (no data)")
                st.session_state.current_page = "extract"
                st.rerun()
        
        with col2:
            if st.button("📊 View Sample Data", use_container_width=True):
                logger.log_user_action("Navigate to Extract samples from Dashboard")
                # You could set a flag to show sample data section
                st.session_state.current_page = "extract"
                st.rerun()
        
        with col3:
            if st.button("🏠 Back to Home", use_container_width=True):
                logger.log_user_action("Navigate to Home from Dashboard")
                st.session_state.current_page = "home"
                st.rerun()
    
    # System information footer
    st.markdown("---")
    with st.expander("ℹ️ System Information"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**FlowForge Version:** 1.0.0")
            st.write("**Runtime:** Streamlit")
            st.write("**Data Processing:** Pandas")
        
        with col2:
            st.write("**Visualization:** Plotly")
            st.write("**Database:** SQLite")
            st.write("**Export Formats:** CSV, Excel, JSON")
    
    # Log dashboard visit
    logger.log_user_action("Visited dashboard page")