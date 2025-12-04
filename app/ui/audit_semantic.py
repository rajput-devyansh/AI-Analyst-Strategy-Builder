import streamlit as st
import time
import polars as pl
from app.deep_scanner import get_batches, analyze_batch, aggregate_deep_issues
from app.agents.janitor import run_janitor
from app.utils import get_data_profile
from app.state_manager import save_checkpoint

def render_deep_scan_tab(df):
    """
    Renders Tab 3: Semantic AI Scan (Logic & Context).
    """
    st.info("🧠 **AI Semantic Scan**: Detects complex paradoxes (e.g., 'Age 5 Married', 'Arrived before Shipped'). This requires AI processing.")
    
    # --- SAMPLING CONTROLS ---
    col_mode, col_input = st.columns([1, 2])
    
    with col_mode:
        mode = st.radio("Sampling Method:", ["Row Count", "Percentage"], label_visibility="collapsed")
    
    with col_input:
        limit = 100
        if mode == "Row Count":
            limit = st.number_input("Max Rows to Scan:", min_value=20, max_value=df.height, value=min(100, df.height))
        else:
            pct = st.slider("Percentage of Data:", 1, 100, 10)
            limit = int(df.height * (pct / 100))
            st.caption(f"Scanning {limit} rows.")

    # --- RUNNER WITH LIVE TIMER ---
    if st.button("Start AI Logic Scan", type="primary", use_container_width=True):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        raw_issues = []
        batch_size = 20
        # Create batches
        batches = list(get_batches(df.head(limit), batch_size))
        total_batches = len(batches)
        
        start_time = time.time()
        
        # Loop through batches
        for i, batch in enumerate(batches):
            batch_start = time.time()
            
            # CALL AI ENGINE
            raw_issues.extend(analyze_batch(batch))
            
            # LIVE TIME ESTIMATION
            batch_duration = time.time() - batch_start
            batches_left = total_batches - (i + 1)
            est_time_left = batch_duration * batches_left
            
            # Update Progress UI
            progress = (i + 1) / total_batches
            progress_bar.progress(progress)
            
            mins, secs = divmod(est_time_left, 60)
            status_text.write(f"Scanning Batch {i+1}/{total_batches}... (Est. Remaining: {int(mins)}m {int(secs)}s)")
        
        total_dur = time.time() - start_time
        
        # Aggregate Results
        issues = aggregate_deep_issues(raw_issues)
        st.session_state["deep_issues"] = issues
        
        # Set Baseline for KPI Arrows
        if st.session_state["deep_count_start"] == 0:
            st.session_state["deep_count_start"] = sum([x['count'] for x in issues])
        
        save_checkpoint(
            "Logic Scan Run", 
            f"Scanned {limit} rows. Found {len(issues)} issues.", 
            "Audit", "Scan", 
            changeset=f"Time: {total_dur:.1f}s"
        )
        status_text.success(f"Scan Complete! ({total_dur:.1f}s)")
        st.rerun()

    # --- RESULTS DISPLAY ---
    if st.session_state["deep_issues"]:
        c_clear, _ = st.columns([1, 4])
        with c_clear:
            if st.button("Clear Results"): 
                st.session_state["deep_issues"] = []
                st.rerun()

        for i, issue in enumerate(st.session_state["deep_issues"]):
            with st.expander(f"⚠️ {issue['issue']} in {issue['column']} (Found {issue['count']} times)"):
                st.write(f"Sample Row Indices: {issue['rows']}")
                
                unique_key_deep = f"deep_{issue['column']}_{i}"
                
                if st.button("Fix Logic", key=f"btn_{unique_key_deep}"):
                    with st.spinner("Janitor writing logic fix..."):
                        profile = get_data_profile(df)
                        
                        # Context-Aware Prompt
                        prompt = f"Fix this logical error: '{issue['issue']}' in column '{issue['column']}'. Use df.filter or df.with_columns."
                        code = run_janitor(profile, issue['issue'], prompt)
                        
                        try:
                            loc = {'pl': pl}
                            exec(code, globals(), loc)
                            new_df = loc['clean_data'](df)
                            
                            diff = df.height - new_df.height
                            changeset = f"Dropped {diff} rows" if diff > 0 else "Logic Updated"
                            
                            st.session_state["df"] = new_df
                            
                            save_checkpoint(
                                f"Fixed {issue['column']}", 
                                f"Logic: {issue['issue']}", 
                                "Audit", "Logic", 
                                changeset
                            )
                            st.success("Fixed!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")