"""
Custom styling and branding utilities for FlowForge
"""

import streamlit as st
from pathlib import Path
from utils.config import THEME_COLORS, ASSETS_DIR

def load_custom_css():
    """Load custom CSS styling"""
    css_file = ASSETS_DIR / "styles" / "custom.css"
    
    if css_file.exists():
        with open(css_file, 'r', encoding='utf-8') as f:
            css_content = f.read()
        st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

def create_logo_header(title: str = "🌊 FlowForge", subtitle: str = "Interactive ETL Platform"):
    """Create a branded header with logo"""
    st.markdown(f"""
    <div class="main-header">
        <h1>{title}</h1>
        <p>{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)

def create_step_indicator(steps: list, current_step: int = 0, completed_steps: list = None):
    """Create a visual step indicator"""
    if completed_steps is None:
        completed_steps = []
    
    step_html = '<div class="step-indicator">'
    
    for i, step in enumerate(steps):
        if step in completed_steps:
            step_class = "step completed"
        elif i == current_step:
            step_class = "step active"
        else:
            step_class = "step"
        
        step_html += f'<div class="{step_class}">{step}</div>'
    
    step_html += '</div>'
    
    st.markdown(step_html, unsafe_allow_html=True)

def create_metric_card(title: str, value: str, description: str = "", delta: str = None):
    """Create a styled metric card"""
    delta_html = ""
    if delta:
        delta_color = "#10B981" if delta.startswith("+") else "#EF4444"
        delta_html = f'<div style="color: {delta_color}; font-size: 0.9rem; font-weight: 500;">{delta}</div>'
    
    card_html = f"""
    <div class="metric-card">
        <h3 style="margin: 0; color: {THEME_COLORS['primary_blue']}; font-size: 1.2rem;">{title}</h3>
        <div style="font-size: 2rem; font-weight: 700; margin: 0.5rem 0; color: {THEME_COLORS['text_dark']};">{value}</div>
        {delta_html}
        <div style="color: {THEME_COLORS['medium_gray']}; font-size: 0.9rem;">{description}</div>
    </div>
    """
    
    st.markdown(card_html, unsafe_allow_html=True)

def create_info_box(message: str, box_type: str = "info"):
    """Create styled info boxes"""
    box_classes = {
        "info": "info-box",
        "success": "success-box", 
        "warning": "warning-box",
        "error": "error-box"
    }
    
    icons = {
        "info": "ℹ️",
        "success": "✅",
        "warning": "⚠️", 
        "error": "❌"
    }
    
    box_class = box_classes.get(box_type, "info-box")
    icon = icons.get(box_type, "ℹ️")
    
    st.markdown(f"""
    <div class="{box_class}">
        {icon} {message}
    </div>
    """, unsafe_allow_html=True)

def create_progress_ring(percentage: float, size: int = 100):
    """Create a circular progress indicator"""
    circumference = 2 * 3.14159 * 45  # radius = 45
    stroke_dashoffset = circumference - (percentage / 100 * circumference)
    
    progress_html = f"""
    <div style="display: flex; justify-content: center; margin: 1rem 0;">
        <svg width="{size}" height="{size}">
            <circle cx="50" cy="50" r="45" stroke="#E5E7EB" stroke-width="8" fill="none" />
            <circle 
                cx="50" 
                cy="50" 
                r="45" 
                stroke="{THEME_COLORS['primary_blue']}" 
                stroke-width="8" 
                fill="none"
                stroke-dasharray="{circumference}"
                stroke-dashoffset="{stroke_dashoffset}"
                stroke-linecap="round"
                style="transition: stroke-dashoffset 0.5s ease-in-out;"
            />
            <text 
                x="50" 
                y="50" 
                text-anchor="middle" 
                dominant-baseline="central" 
                fill="{THEME_COLORS['text_dark']}"
                font-size="16"
                font-weight="bold"
            >
                {percentage:.0f}%
            </text>
        </svg>
    </div>
    """
    
    st.markdown(progress_html, unsafe_allow_html=True)

def create_feature_card(icon: str, title: str, description: str, button_text: str = None, button_key: str = None):
    """Create a feature highlight card"""
    button_html = ""
    if button_text and button_key:
        button_html = f"""
        <button 
            onclick="document.querySelector('[data-testid=\\"button\\"][key=\\"{button_key}\\"]').click()"
            style="
                background: {THEME_COLORS['primary_blue']};
                color: white;
                border: none;
                border-radius: 6px;
                padding: 0.5rem 1rem;
                font-weight: 500;
                cursor: pointer;
                margin-top: 1rem;
            "
        >
            {button_text}
        </button>
        """
    
    card_html = f"""
    <div class="card" style="text-align: center; height: 100%;">
        <div style="font-size: 3rem; margin-bottom: 1rem;">{icon}</div>
        <h3 style="color: {THEME_COLORS['primary_blue']}; margin-bottom: 1rem;">{title}</h3>
        <p style="color: {THEME_COLORS['text_dark']}; line-height: 1.6;">{description}</p>
        {button_html}
    </div>
    """
    
    st.markdown(card_html, unsafe_allow_html=True)

def create_timeline_item(title: str, description: str, timestamp: str, status: str = "completed"):
    """Create a timeline item for showing process history"""
    status_colors = {
        "completed": THEME_COLORS["success_green"],
        "active": THEME_COLORS["accent_orange"], 
        "pending": THEME_COLORS["medium_gray"]
    }
    
    status_icons = {
        "completed": "✅",
        "active": "🔄",
        "pending": "⏳"
    }
    
    color = status_colors.get(status, THEME_COLORS["medium_gray"])
    icon = status_icons.get(status, "⏳")
    
    timeline_html = f"""
    <div style="
        display: flex;
        align-items: flex-start;
        margin-bottom: 1.5rem;
        padding: 1rem;
        border-left: 3px solid {color};
        background: rgba(255, 255, 255, 0.8);
        border-radius: 0 8px 8px 0;
    ">
        <div style="
            background: {color};
            color: white;
            border-radius: 50%;
            width: 30px;
            height: 30px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-right: 1rem;
            flex-shrink: 0;
        ">
            {icon}
        </div>
        <div style="flex-grow: 1;">
            <h4 style="margin: 0; color: {THEME_COLORS['text_dark']};">{title}</h4>
            <p style="margin: 0.5rem 0; color: {THEME_COLORS['medium_gray']};">{description}</p>
            <small style="color: {THEME_COLORS['medium_gray']};">{timestamp}</small>
        </div>
    </div>
    """
    
    st.markdown(timeline_html, unsafe_allow_html=True)

def create_data_quality_badge(score: float):
    """Create a data quality badge"""
    if score >= 90:
        color = THEME_COLORS["success_green"]
        label = "Excellent"
        icon = "🟢"
    elif score >= 75:
        color = THEME_COLORS["accent_orange"] 
        label = "Good"
        icon = "🟡"
    elif score >= 60:
        color = THEME_COLORS["warning_yellow"]
        label = "Fair"
        icon = "🟠"
    else:
        color = THEME_COLORS["error_red"]
        label = "Poor"
        icon = "🔴"
    
    badge_html = f"""
    <div style="
        display: inline-flex;
        align-items: center;
        background: {color};
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        margin: 0.5rem 0;
    ">
        {icon} {label} ({score:.1f}/100)
    </div>
    """
    
    st.markdown(badge_html, unsafe_allow_html=True)

def apply_custom_theme():
    """Apply custom theme and hide Streamlit branding"""
    st.markdown("""
    <style>
        /* Hide Streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: #F3F4F6;
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb {
            background: #9CA3AF;
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: #1E3A8A;
        }
        
        /* Improve sidebar */
        .css-1d391kg {
            background: linear-gradient(180deg, #F3F4F6 0%, white 100%);
        }
        
        /* Better button hover effects */
        .stButton > button:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(30, 58, 138, 0.15);
        }
        
        /* Improve file uploader */
        .stFileUploader > div {
            border: 2px dashed #E5E7EB;
            border-radius: 12px;
            background: #F9FAFB;
            transition: all 0.3s ease;
        }
        
        .stFileUploader > div:hover {
            border-color: #1E3A8A;
            background: rgba(30, 58, 138, 0.05);
        }
    </style>
    """, unsafe_allow_html=True)

def show_loading_animation(message: str = "Processing..."):
    """Show a loading animation"""
    loading_html = f"""
    <div style="
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 2rem;
    ">
        <div style="
            width: 40px;
            height: 40px;
            border: 4px solid #E5E7EB;
            border-top: 4px solid {THEME_COLORS['primary_blue']};
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-bottom: 1rem;
        "></div>
        <p style="color: {THEME_COLORS['text_dark']}; font-weight: 500;">{message}</p>
    </div>
    
    <style>
        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
    </style>
    """
    
    st.markdown(loading_html, unsafe_allow_html=True)