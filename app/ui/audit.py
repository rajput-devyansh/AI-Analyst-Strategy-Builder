import streamlit as st
from app.schema_manager import get_current_schema_view
# Import the separated UI modules
from app.ui.audit_fast import render_fast_scan_tab
from app.ui.audit_advanced import render_advanced_tab
from app.ui.audit_semantic import render_deep_scan_tab

def render_audit_page():
    st.title("🕵️ Step 3: AI Audit")
    
    df = st.session_state["df"]
    
    # --- METRICS CALCULATION ---
    # 1. Structural (Fast Scan)
    struct_current = len(st.session_state.get("fast_issues", []))
    
    # 2. Advanced (Vocab + Stats)
    adv_current = len(st.session_state.get("vocab_issues", [])) + \
                  len(st.session_state.get("stat_issues", []))
    
    # 3. Logic (Deep AI Scan) - Sum of 'count' inside issues
    logic_current = sum([i['count'] for i in st.session_state.get("deep_issues", [])])
    
    # Diff Calculation (Current - Start)
    # Note: We haven't set baselines for Advanced yet, so we just show current count for now
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
                    # Reset all caches
                    st.session_state["fast_issues"] = [] 
                    st.session_state["vocab_issues"] = []
                    st.session_state["stat_issues"] = []
                    st.session_state["deep_issues"] = []
                    st.session_state["ignored_issues"] = set()
                    st.rerun()

    # --- DISPLAY METRICS ---
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Rows", df.height)
    m2.metric("Columns", df.width)
    m3.metric("Structural", struct_current, delta=f"{diff_struct}" if diff_struct != 0 else None, delta_color="inverse")
    m4.metric("Advanced", adv_current, delta_color="inverse") # Vocabulary + Stats
    m5.metric("Logic", logic_current, delta=f"{diff_logic}" if diff_logic != 0 else None, delta_color="inverse")

    st.divider()
    
    # --- TABS ---
    # We now have 3 tabs as planned
    tab1, tab2, tab3 = st.tabs(["⚡ Structural (Fast)", "🔎 Advanced (Code)", "🧠 Semantic (AI)"])

    with tab1:
        render_fast_scan_tab(df)

    with tab2:
        render_advanced_tab(df) # Vocab + Statistical
        
    with tab3:
        render_deep_scan_tab(df) # AI Logic