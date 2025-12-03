import streamlit as st
from app.schema_manager import get_current_schema_view
# Import the new separated tabs
from app.ui.audit_fast import render_fast_scan_tab
from app.ui.audit_deep import render_deep_scan_tab

def render_audit_page():
    st.title("🕵️ Step 3: AI Audit")
    
    df = st.session_state["df"]
    
    # --- METRICS CALCULATION ---
    struct_current = len(st.session_state["fast_issues"])
    logic_current = sum([i['count'] for i in st.session_state["deep_issues"]]) if st.session_state["deep_issues"] else 0
    
    diff_struct = struct_current - st.session_state.get("fast_count_start", 0)
    diff_logic = logic_current - st.session_state.get("deep_count_start", 0)

    # --- HEADER: CONFIRMED SCHEMA ---
    with st.expander("✅ Confirmed Schema (Click to Edit)", expanded=False):
        c_a, c_b = st.columns([4, 1])
        with c_a:
            st.dataframe(get_current_schema_view(df), use_container_width=True, hide_index=True)
        with c_b:
            st.write("") 
            if st.button("✏️ Edit Schema", type="secondary"):
                st.warning("Going back will undo all Audit progress!")
                if st.button("Confirm Go Back"):
                    st.session_state["app_stage"] = "SCHEMA"
                    st.session_state["schema_locked"] = False
                    st.session_state["fast_issues"] = [] 
                    st.session_state["deep_issues"] = []
                    st.session_state["ignored_issues"] = set()
                    st.rerun()

    # --- DISPLAY METRICS ---
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Rows", df.height)
    m2.metric("Columns", df.width)
    m3.metric("Structural Issues", struct_current, delta=f"{diff_struct}" if diff_struct != 0 else None, delta_color="inverse")
    m4.metric("Logic Issues", logic_current, delta=f"{diff_logic}" if diff_logic != 0 else None, delta_color="inverse")

    st.divider()
    
    # --- TABS ---
    tab1, tab2 = st.tabs(["⚡ Fast Scan (Structural)", "🧠 Deep Scan (Logic)"])

    with tab1:
        render_fast_scan_tab(df)

    with tab2:
        render_deep_scan_tab(df)