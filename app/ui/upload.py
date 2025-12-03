import streamlit as st
from app.utils import load_data
from app.schema_manager import get_current_schema_view
from app.state_manager import save_checkpoint

def render_upload_page():
    st.title("📂 Step 1: Data Ingestion")
    st.markdown("Upload your raw dataset to begin the audit pipeline.")
    
    uploaded_file = st.file_uploader("Drop file here", type=["csv", "xlsx"])
    
    if uploaded_file:
        if st.button("🚀 Load & Initialize", type="primary"):
            df = load_data(uploaded_file)
            if isinstance(df, str):
                st.error(df)
            else:
                st.session_state["df"] = df
                st.session_state["initial_schema_view"] = get_current_schema_view(df)
                
                # Checkpoints
                save_checkpoint("Data Loaded", f"File: {uploaded_file.name} | Rows: {df.height}", "Ingestion")
                save_checkpoint("Initial Schema Detected", "Polars inferred types.", "Schema", "System")
                
                st.session_state["app_stage"] = "SCHEMA"
                st.rerun()