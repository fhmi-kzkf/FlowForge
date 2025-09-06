"""
FlowForge - Interactive ETL Platform
Main Streamlit Application Entry Point
"""

import streamlit as st
import sys
from pathlib import Path

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

from utils.config import APP_TITLE, APP_SUBTITLE, APP_ICON, THEME_COLORS
from utils.helpers import initialize_session_state
from utils.logger import logger
from utils.styling import load_custom_css, apply_custom_theme

# Import pages
from pages.home import show_home_page
from pages.extract_page import show_extract_page
from pages.transform_page import show_transform_page
from pages.load_page import show_load_page
from pages.dashboard import show_dashboard_page

def configure_page():
    """Configure Streamlit page settings"""
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=APP_ICON,
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Hide Streamlit's default page navigation to avoid double navbar
    st.markdown("""
    <style>
        .stAppHeader { display: none; }
        section[data-testid="stSidebar"] .css-ng1t4o {{display: none}}
        section[data-testid="stSidebar"] .css-1d391kg {{display: none}}
    </style>
    """, unsafe_allow_html=True)

def load_custom_css():
    """Load custom CSS styling"""
    # This function is now in styling.py
    pass

def create_navigation():
    """Create navigation sidebar"""
    with st.sidebar:
        # Header with better styling
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {THEME_COLORS['primary_blue']} 0%, {THEME_COLORS['accent_orange']} 100%);
            padding: 1.5rem;
            margin: -1rem -1rem 2rem -1rem;
            border-radius: 0 0 15px 15px;
            text-align: center;
            color: white;
        ">
            <h1 style="margin: 0; font-size: 1.8rem;">{APP_ICON} FlowForge</h1>
            <p style="margin: 0.5rem 0 0 0; opacity: 0.9; font-size: 0.9rem;">{APP_SUBTITLE}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Navigation menu with better styling
        st.markdown("### 🧭 Navigation")
        
        page_options = {
            "🏠 Home": "home",
            "📂 Extract Data": "extract",
            "🔍 Transform Data": "transform", 
            "💾 Load Data": "load",
            "📊 Dashboard": "dashboard"
        }
        
        # Create navigation buttons instead of selectbox
        selected_page = None
        
        for page_name, page_key in page_options.items():
            # Check if this is the current page
            current_page = st.session_state.get('current_page', 'home')
            
            # Create button with conditional styling
            if page_key == current_page:
                button_type = "primary"
            else:
                button_type = "secondary"
                
            if st.button(page_name, key=f"nav_{page_key}", use_container_width=True, type=button_type):
                selected_page = page_key
                st.session_state.current_page = page_key
                st.rerun()
        
        # If no button was clicked, use current page or default to home
        if selected_page is None:
            selected_page = st.session_state.get('current_page', 'home')
        
        st.markdown("---")
        
        # Progress indicator
        if st.session_state.get("progress_tracker"):
            st.markdown("### 📋 ETL Progress")
            tracker = st.session_state.progress_tracker
            if tracker and "completed_steps" in tracker and "steps" in tracker:
                progress = len(tracker["completed_steps"]) / len(tracker["steps"])
                st.progress(progress)
                st.write(f"**{len(tracker['completed_steps'])}/{len(tracker['steps'])}** steps completed")
                
                # Show current step
                if len(tracker['completed_steps']) < len(tracker['steps']):
                    current_step_idx = len(tracker['completed_steps'])
                    current_step = tracker['steps'][current_step_idx]
                    st.info(f"Current: {current_step}")
        
        # Quick stats
        if st.session_state.get("current_data") is not None:
            st.markdown("### 📈 Dataset Info")
            df = st.session_state.current_data
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Rows", f"{len(df):,}")
            with col2:
                st.metric("Columns", len(df.columns))
                
            # Data quality indicator
            null_count = df.isnull().sum().sum()
            completeness = ((len(df) * len(df.columns) - null_count) / (len(df) * len(df.columns))) * 100
            
            if completeness >= 95:
                st.success(f"Data Quality: {completeness:.1f}%")
            elif completeness >= 80:
                st.warning(f"Data Quality: {completeness:.1f}%")
            else:
                st.error(f"Data Quality: {completeness:.1f}%")
        
        return selected_page

def main():
    """Main application function"""
    # Configure page
    configure_page()
    
    # Load custom CSS and apply theme
    load_custom_css()
    apply_custom_theme()
    
    # Initialize session state
    initialize_session_state()
    
    # Create navigation and get selected page
    selected_page = create_navigation()
    
    # Log user navigation
    logger.log_user_action(f"Navigate to {selected_page} page")
    
    # Display selected page
    try:
        if selected_page == "home":
            show_home_page()
        elif selected_page == "extract":
            show_extract_page()
        elif selected_page == "transform":
            show_transform_page()
        elif selected_page == "load":
            show_load_page()
        elif selected_page == "dashboard":
            show_dashboard_page()
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        logger.log_etl_step("ERROR", f"Page error in {selected_page}: {str(e)}")

if __name__ == "__main__":
    main()