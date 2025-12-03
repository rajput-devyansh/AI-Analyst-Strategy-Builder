import streamlit as st
import polars as pl
from app.utils import get_data_profile
from app.deep_scanner import get_batches, analyze_batch, aggregate_deep_issues
from app.agents.janitor import run_janitor
from app.state_manager import save_checkpoint

def render_deep_scan_tab(df):
    """Renders the Logic/Semantic Scan UI."""
    
    col_scan_1, col_scan_2 = st.columns([1, 3])
    limit = col_scan_1.number_input("Rows to Scan", 100, 5000, 100)
    
    if col_scan_2.button("Start AI Logic Scan", use_container_width=True):
        progress = st.progress(0)
        status = st.empty()
        raw_issues = []
        batches = list(get_batches(df.head(limit), 20))
        
        for i, batch in enumerate(batches):
            progress.progress((i+1)/len(batches))
            status.caption(f"Scanning Batch {i+1}/{len(batches)}...")
            raw_issues.extend(analyze_batch(batch))
        
        issues = aggregate_deep_issues(raw_issues)
        st.session_state["deep_issues"] = issues
        
        if st.session_state["deep_count_start"] == 0:
            st.session_state["deep_count_start"] = sum([x['count'] for x in issues])
        
        save_checkpoint("Logic Scan Run", f"Found {len(issues)} anomalies.", "Audit", "Scan")
        status.success("Complete!")
        st.rerun()

    if st.session_state["deep_issues"]:
        if st.button("Clear Results"): st.session_state["deep_issues"] = []

    # Render Cards
    for i, issue in enumerate(st.session_state["deep_issues"]):
        with st.expander(f"⚠️ {issue['issue']} in {issue['column']} (Found {issue['count']} times)"):
            st.write(f"Sample Row Indices: {issue['rows']}")
            
            unique_key_deep = f"deep_{issue['column']}_{i}"
            
            if st.button("Fix Logic", key=f"btn_{unique_key_deep}"):
                with st.spinner("Janitor Working..."):
                    profile = get_data_profile(df)
                    code = run_janitor(profile, issue['issue'], f"Fix: {issue['issue']}")
                    try:
                        loc = {'pl': pl}
                        exec(code, globals(), loc)
                        st.session_state["df"] = loc['clean_data'](df)
                        
                        save_checkpoint(f"AI Fix: {issue['column']}", f"Logic Fixed: {issue['issue']}", "Audit", "Logic")
                        st.success("Fixed!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")