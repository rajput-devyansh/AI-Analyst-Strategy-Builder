import streamlit as st
from app.state_manager import init_session_state

# --- IMPORT UI MODULES ---
from app.ui.sidebar import render_sidebar
from app.ui.upload import render_upload_page
from app.ui.schema import render_schema_page
from app.ui.audit import render_audit_page

# --- APP SETUP ---
st.set_page_config(page_title="AI Data Auditor", layout="wide", page_icon="🧠")
init_session_state()

# --- 1. RENDER SIDEBAR ---
render_sidebar()

# --- 2. ROUTER LOGIC ---
stage = st.session_state["app_stage"]

if stage == "UPLOAD":
    render_upload_page()

elif stage == "SCHEMA":
    render_schema_page()

elif stage == "AUDIT":
    render_audit_page()

# --- 3. GLOBAL FOOTER (Optional Preview) ---
if st.session_state["df"] is not None:
    st.markdown("---")
    st.subheader("📊 Live Data Preview")
    st.dataframe(st.session_state["df"].head(50).to_pandas(), use_container_width=True)