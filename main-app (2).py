"""Main application entry point with modular UI components."""

import streamlit as st
from typing import Dict, Any

# Import configuration and logging
from config.logging_config import get_module_logger

# Import UI components
from ui.components import (
    set_page_config, render_header, render_footer,
    render_sidebar, render_chat_tab, render_iep_tab, render_lesson_plan_tab,
    display_error
)
from ui.components.visualization import render_analytics_tab

# Import application initialization
from main import load_app_components

# Create a logger for this module
logger = get_module_logger("streamlit_app")

def run_app():
    """Main function to run the application."""
    try:
        # Configure page settings
        set_page_config()
        
        # Display app header
        render_header()
        
        # Initialize application components
        app_components = load_app_components()
        
        if not app_components:
            display_error("Failed to initialize application. Please check the logs.")
            return
        
        # Create sidebar for file upload and system status
        render_sidebar(app_components)
        
        # Create main content tabs
        tab1, tab2, tab3, tab4 = st.tabs(["Chat", "IEP Generation", "Lesson Plans", "Analytics"])
        
        # Populate tabs
        with tab1:
            render_chat_tab(app_components)
        
        with tab2:
            render_iep_tab(app_components)
        
        with tab3:
            render_lesson_plan_tab(app_components)
            
        with tab4:
            render_analytics_tab(app_components)
            
        # Add footer
        render_footer()
        
    except Exception as e:
        logger.error(f"Error in main app: {str(e)}", exc_info=True)
        display_error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    run_app()
