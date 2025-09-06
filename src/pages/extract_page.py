"""
Extract Data page for FlowForge ETL Platform
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from typing import Optional

from modules.extract import extractor
from utils.helpers import (
    display_success_message, display_error_message, display_info_message,
    display_dataframe_info, validate_file_upload, create_progress_tracker,
    update_progress_tracker
)
from utils.logger import logger
from utils.config import SAMPLES_DIR

def show_extract_page():
    """Display the data extraction page"""
    
    st.title("📂 Extract Data")
    st.markdown("Import data from various sources to begin your ETL pipeline")
    
    # Create tabs for different extraction methods
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📁 File Upload", 
        "🌐 API", 
        "🗃️ Database", 
        "📊 Sample Data",
        "✏️ Manual Input"
    ])
    
    # Initialize progress tracker if not exists
    if "progress_tracker" not in st.session_state or st.session_state.progress_tracker is None:
        st.session_state.progress_tracker = create_progress_tracker([
            "Extract", "Transform", "Load", "Complete"
        ])
    
    extracted_data = None
    
    # Tab 1: File Upload
    with tab1:
        st.header("📁 Upload File")
        st.markdown("Support for CSV, Excel (.xlsx, .xls), and JSON files")
        
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=['csv', 'xlsx', 'xls', 'json'],
            help="Maximum file size: 100MB"
        )
        
        if uploaded_file:
            # Validate file
            is_valid, validation_msg = validate_file_upload(uploaded_file)
            
            if not is_valid:
                display_error_message(validation_msg)
            else:
                display_info_message(validation_msg)
                
                # Show file info
                file_size_mb = uploaded_file.size / (1024 * 1024)
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("File Name", uploaded_file.name)
                with col2:
                    st.metric("File Size", f"{file_size_mb:.2f} MB")
                with col3:
                    st.metric("File Type", uploaded_file.type)
                
                # Extract button
                if st.button("🔍 Extract Data", key="extract_file", type="primary"):
                    with st.spinner("Extracting data from file..."):
                        extracted_data, message = extractor.extract_from_file(uploaded_file)
                        
                        if extracted_data is not None:
                            display_success_message(message)
                            st.session_state.extracted_data = extracted_data
                            st.session_state.current_data = extracted_data
                            update_progress_tracker(st.session_state.progress_tracker, "Extract")
                        else:
                            display_error_message(message)
    
    # Tab 2: API Integration
    with tab2:
        st.header("🌐 API Data Source")
        st.markdown("Fetch data from public APIs")
        
        # Popular APIs section
        st.subheader("📋 Popular APIs")
        popular_apis = extractor.get_popular_apis()
        
        selected_api = st.selectbox(
            "Choose a sample API:",
            options=[""] + list(popular_apis.keys()),
            help="Select a pre-configured API for quick testing"
        )
        
        if selected_api:
            api_config = popular_apis[selected_api]
            st.info(f"**{api_config['type']}**: {api_config['description']}")
            api_url = api_config['url']
        else:
            api_url = ""
        
        # Custom API section
        st.subheader("🔧 Custom API")
        custom_url = st.text_input(
            "API URL:", 
            value=api_url,
            placeholder="https://api.example.com/data"
        )
        
        # Advanced options
        with st.expander("⚙️ Advanced API Options"):
            headers_text = st.text_area(
                "Headers (JSON format):",
                placeholder='{"Authorization": "Bearer token", "Content-Type": "application/json"}',
                help="Optional HTTP headers in JSON format"
            )
            
            params_text = st.text_area(
                "Parameters (JSON format):",
                placeholder='{"limit": 100, "format": "json"}',
                help="Optional query parameters in JSON format"
            )
        
        if st.button("🌐 Fetch Data", key="extract_api", type="primary"):
            if not custom_url.strip():
                display_error_message("Please provide an API URL")
            else:
                with st.spinner("Fetching data from API..."):
                    # Parse headers and params
                    headers = {}
                    params = {}
                    
                    if headers_text.strip():
                        try:
                            import json
                            headers = json.loads(headers_text)
                        except json.JSONDecodeError:
                            display_error_message("Invalid JSON format in headers")
                            st.stop()
                    
                    if params_text.strip():
                        try:
                            import json
                            params = json.loads(params_text)
                        except json.JSONDecodeError:
                            display_error_message("Invalid JSON format in parameters")
                            st.stop()
                    
                    extracted_data, message = extractor.extract_from_api(
                        custom_url, headers=headers, params=params
                    )
                    
                    if extracted_data is not None:
                        display_success_message(message)
                        st.session_state.extracted_data = extracted_data
                        st.session_state.current_data = extracted_data
                        update_progress_tracker(st.session_state.progress_tracker, "Extract")
                    else:
                        display_error_message(message)
    
    # Tab 3: Database
    with tab3:
        st.header("🗃️ SQLite Database")
        st.markdown("Extract data from SQLite database files")
        
        # Database file upload
        db_file = st.file_uploader(
            "Upload SQLite Database File (.db, .sqlite)",
            type=['db', 'sqlite', 'sqlite3']
        )
        
        if db_file:
            # Save uploaded file temporarily
            temp_db_path = f"temp_{db_file.name}"
            with open(temp_db_path, "wb") as f:
                f.write(db_file.read())
            
            # List tables
            tables, table_message = extractor.list_sqlite_tables(temp_db_path)
            
            if tables:
                display_info_message(table_message)
                
                # Table selection
                selected_table = st.selectbox(
                    "Select Table:",
                    options=tables,
                    help="Choose a table to extract data from"
                )
                
                # Custom query option
                use_custom_query = st.checkbox("Use Custom SQL Query")
                
                if use_custom_query:
                    custom_query = st.text_area(
                        "SQL Query:",
                        placeholder="SELECT * FROM table_name WHERE condition",
                        help="Write your custom SQL query"
                    )
                else:
                    custom_query = None
                
                if st.button("🗃️ Extract from Database", key="extract_db", type="primary"):
                    with st.spinner("Extracting data from database..."):
                        if use_custom_query and custom_query:
                            extracted_data, message = extractor.extract_from_sqlite(
                                temp_db_path, query=custom_query
                            )
                        else:
                            extracted_data, message = extractor.extract_from_sqlite(
                                temp_db_path, table_name=selected_table
                            )
                        
                        if extracted_data is not None:
                            display_success_message(message)
                            st.session_state.extracted_data = extracted_data
                            st.session_state.current_data = extracted_data
                            update_progress_tracker(st.session_state.progress_tracker, "Extract")
                        else:
                            display_error_message(message)
                    
                    # Clean up temp file
                    try:
                        Path(temp_db_path).unlink()
                    except:
                        pass
            else:
                display_error_message(table_message)
    
    # Tab 4: Sample Data
    with tab4:
        st.header("📊 Sample Datasets")
        st.markdown("Use built-in sample datasets for learning and testing")
        
        # Sample datasets info
        sample_datasets = {
            "titanic": {
                "name": "🚢 Titanic Dataset", 
                "description": "Historic passenger data from the Titanic",
                "rows": "891 rows",
                "features": "Survival, class, age, gender, etc."
            },
            "sales": {
                "name": "💰 Sales Transactions",
                "description": "Retail sales transaction records", 
                "rows": "1000 rows",
                "features": "Product, price, quantity, date, etc."
            },
            "weather": {
                "name": "🌤️ Weather Data",
                "description": "Daily weather measurements",
                "rows": "365 rows", 
                "features": "Temperature, humidity, pressure, etc."
            },
            "customers": {
                "name": "👥 Customer Demographics",
                "description": "Customer information and preferences",
                "rows": "500 rows",
                "features": "Age, income, location, preferences, etc."
            }
        }
        
        # Display sample options
        for sample_key, sample_info in sample_datasets.items():
            with st.container():
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.markdown(f"**{sample_info['name']}**")
                    st.markdown(sample_info['description'])
                
                with col2:
                    st.markdown(f"📊 {sample_info['rows']}")
                    st.markdown(f"🔧 {sample_info['features']}")
                
                with col3:
                    if st.button("Load", key=f"load_{sample_key}"):
                        with st.spinner(f"Loading {sample_info['name']}..."):
                            extracted_data, message = extractor.extract_sample_data(sample_key)
                            
                            if extracted_data is not None:
                                display_success_message(message)
                                st.session_state.extracted_data = extracted_data
                                st.session_state.current_data = extracted_data
                                update_progress_tracker(st.session_state.progress_tracker, "Extract")
                            else:
                                display_error_message(message)
                
                st.markdown("---")
    
    # Tab 5: Manual Input
    with tab5:
        st.header("✏️ Manual Data Input")
        st.markdown("Enter CSV data directly")
        
        # CSV text input
        csv_text = st.text_area(
            "Paste CSV Data:",
            placeholder="name,age,city\nJohn,25,New York\nJane,30,Los Angeles\nBob,35,Chicago",
            height=200,
            help="Enter CSV data with headers in the first row"
        )
        
        # Delimiter selection
        delimiter = st.selectbox(
            "Delimiter:",
            options=[",", ";", "\t", "|"],
            format_func=lambda x: {"," : "Comma (,)", ";" : "Semicolon (;)", "\t" : "Tab", "|" : "Pipe (|)"}[x]
        )
        
        if st.button("📝 Parse CSV", key="extract_manual", type="primary"):
            if not csv_text.strip():
                display_error_message("Please enter some CSV data")
            else:
                with st.spinner("Parsing CSV data..."):
                    extracted_data, message = extractor.extract_from_csv_text(csv_text, delimiter)
                    
                    if extracted_data is not None:
                        display_success_message(message)
                        st.session_state.extracted_data = extracted_data
                        st.session_state.current_data = extracted_data
                        update_progress_tracker(st.session_state.progress_tracker, "Extract")
                    else:
                        display_error_message(message)
    
    # Display extracted data preview
    if st.session_state.get("current_data") is not None:
        st.markdown("---")
        st.header("📋 Extracted Data Preview")
        
        df = st.session_state.current_data
        
        # Display data info
        display_dataframe_info(df, "📊 Dataset Overview")
        
        # Data preview
        st.subheader("🔍 Data Sample")
        
        # Preview options
        col1, col2 = st.columns([1, 1])
        with col1:
            preview_rows = st.slider("Rows to display:", 5, min(100, len(df)), 10)
        with col2:
            show_all_columns = st.checkbox("Show all columns", value=False)
        
        # Display data
        if show_all_columns:
            st.dataframe(df.head(preview_rows), use_container_width=True)
        else:
            # Show first few columns if many columns
            if len(df.columns) > 10:
                st.dataframe(df.iloc[:, :10].head(preview_rows), use_container_width=True)
                st.info(f"Showing first 10 of {len(df.columns)} columns. Check 'Show all columns' to see more.")
            else:
                st.dataframe(df.head(preview_rows), use_container_width=True)
        
        # Next step button
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button("➡️ Continue to Transform", type="primary", use_container_width=True):
                logger.log_user_action("Navigate to Transform from Extract")
                st.session_state.current_page = "transform"
                st.rerun()
    
    else:
        st.info("👆 Choose a data source above to begin extraction")