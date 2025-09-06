"""
Home page for FlowForge ETL Platform
"""

import streamlit as st
from utils.config import APP_TITLE, APP_SUBTITLE, THEME_COLORS
from utils.helpers import display_info_message
from utils.logger import logger

def show_home_page():
    """Display the home/landing page"""
    
    # Main header
    st.markdown(f"""
    <div class="main-header">
        <h1>🌊 Welcome to FlowForge</h1>
        <h3>Interactive ETL Platform for Data Engineering & Learning</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Introduction section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ### 🎯 What is FlowForge?
        
        FlowForge is an interactive web-based platform that makes **Extract, Transform, Load (ETL)** 
        processes accessible to everyone. Whether you're a student learning data engineering concepts, 
        a researcher working with data, or a professional looking for a quick ETL solution, 
        FlowForge provides an intuitive interface to work with your data.
        
        ### ✨ Key Features
        
        #### 📂 **Extract**
        - Upload CSV, Excel, and JSON files
        - Connect to SQLite databases  
        - Fetch data from public APIs
        - Automatic data profiling and metadata analysis
        
        #### 🔍 **Transform**
        - Interactive data cleaning tools
        - Filter and sort your data
        - Handle missing values intelligently
        - Rename, drop, or merge columns
        - Data type conversions
        - Column transformations and calculations
        
        #### 💾 **Load**
        - Export to CSV or Excel formats
        - Save to SQLite databases
        - Direct download functionality
        - Preview results before saving
        
        #### 📊 **Monitor & Visualize**
        - Real-time progress tracking
        - Data preview at each step
        - Comprehensive logging for reproducibility
        - Visual feedback on transformations
        """)
    
    with col2:
        st.markdown("""
        ### 🚀 Quick Start
        
        Ready to begin your ETL journey?
        """)
        
        if st.button("🎯 Start Your First Pipeline", type="primary", use_container_width=True):
            st.session_state.current_page = "extract"
            st.rerun()
        
        st.markdown("---")
        
        st.markdown("""
        ### 📚 Sample Data Available
        
        - 🚢 Titanic Dataset
        - 💰 Sales Transactions
        - 🌤️ Weather Data
        - 👥 Customer Demographics
        """)
        
        if st.button("📊 View Dashboard", use_container_width=True):
            st.session_state.current_page = "dashboard"
            st.rerun()
    
    # ETL Process Overview
    st.markdown("### 🔄 ETL Process Overview")
    
    # Create visual process flow
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background: #E0F2FE; border-radius: 10px;">
            <h3>📂</h3>
            <h4>Extract</h4>
            <p>Import data from various sources</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background: #FEF3C7; border-radius: 10px;">
            <h3>🔍</h3>
            <h4>Transform</h4>
            <p>Clean and prepare your data</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background: #D1FAE5; border-radius: 10px;">
            <h3>💾</h3>
            <h4>Load</h4>
            <p>Save processed data</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background: #F3E8FF; border-radius: 10px;">
            <h3>📊</h3>
            <h4>Monitor</h4>
            <p>Track and visualize progress</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Benefits section
    st.markdown("### 🎯 Why Choose FlowForge?")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **🎓 Learning-Focused**
        - Step-by-step guidance
        - Educational tooltips
        - Best practices built-in
        - Interactive learning
        """)
    
    with col2:
        st.markdown("""
        **🚀 User-Friendly**
        - No coding required
        - Intuitive interface
        - Visual feedback
        - One-click operations
        """)
    
    with col3:
        st.markdown("""
        **🔧 Powerful & Flexible**
        - Multiple data sources
        - Advanced transformations
        - Custom export options
        - Reproducible workflows
        """)
    
    # Getting started tips
    with st.expander("💡 Tips for Getting Started"):
        st.markdown("""
        1. **Start Small**: Begin with a small dataset to familiarize yourself with the interface
        2. **Use Sample Data**: Try our built-in sample datasets to explore features
        3. **Follow the Flow**: Go through Extract → Transform → Load in order for best results
        4. **Check the Logs**: Use the dashboard to review your ETL history
        5. **Experiment**: Don't be afraid to try different transformations!
        
        **Pro Tip**: You can always return to previous steps to modify your pipeline.
        """)
    
    # Log home page visit
    logger.log_user_action("Visited home page")