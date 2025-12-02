import streamlit as st
import polars as pl

# --- IMPORTS ---
from app.utils import load_data, get_data_profile
from app.scanner import scan_structural_issues
from app.structural_fixer import apply_fix 
from app.deep_scanner import get_batches, analyze_batch, aggregate_deep_issues
from app.agents.janitor import run_janitor 
from app.schema_manager import get_column_info, get_preview_value, cast_single_column, get_current_schema_view, TYPE_MAPPING
from app.state_manager import init_session_state, save_checkpoint, restore_checkpoint

# --- SETUP ---
st.set_page_config(page_title="AI Data Auditor", layout="wide", page_icon="🧠")
init_session_state()

# ==============================================================================
# 🕒 SIDEBAR: HISTORY & UNDO
# ==============================================================================
with st.sidebar:
    st.title("🕒 Change History")
    
    if not st.session_state["history"]:
        st.info("No changes applied yet.")
    else:
        st.caption("Click 'Revert' to go back to a previous state.")
        # Iterate backwards to show newest at top
        for i in range(len(st.session_state["history"]) - 1, -1, -1):
            snapshot = st.session_state["history"][i]
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"**{i+1}. {snapshot['desc']}**")
            with col2:
                if st.button("↩️", key=f"hist_{i}", help=f"Revert to before '{snapshot['desc']}'"):
                    restore_checkpoint(i)
                    st.rerun()
        
        st.divider()
        if st.button("♻️ Reset All (Start Over)"):
            st.session_state.clear()
            st.rerun()

# ==============================================================================
# 1. STAGE: UPLOAD (Main Screen)
# ==============================================================================
if st.session_state["app_stage"] == "UPLOAD":
    st.title("📂 Step 1: Upload Raw Data")
    st.markdown("Upload your CSV or Excel file to begin the Audit Process.")
    
    uploaded_file = st.file_uploader("Drag and drop file here", type=["csv", "xlsx"])
    
    if uploaded_file:
        if st.button("🚀 Analyze File", type="primary"):
            df = load_data(uploaded_file)
            if isinstance(df, str):
                st.error(df)
            else:
                # Save Initial State
                st.session_state["df"] = df
                st.session_state["initial_schema_view"] = get_current_schema_view(df)
                
                # Move to Next Stage
                save_checkpoint("Initial Upload") # Save clean state
                st.session_state["app_stage"] = "SCHEMA"
                st.rerun()

# ==============================================================================
# 2. STAGE: SCHEMA MANAGER
# ==============================================================================
elif st.session_state["app_stage"] == "SCHEMA":
    st.title("🛠️ Step 2: Schema Validation")
    st.info("Review data types before we scan for errors. Click 'Update' to apply changes.")
    
    df = st.session_state["df"]
    
    # A. Display Initial vs Current
    col_a, col_b = st.columns(2)
    with col_a:
        with st.expander("📄 Initial Detected Schema"):
            st.dataframe(st.session_state["initial_schema_view"], use_container_width=True, hide_index=True)
    with col_b:
        with st.expander("📄 Current Schema (Live)"):
            st.dataframe(get_current_schema_view(df), use_container_width=True, hide_index=True)

    st.divider()
    
    # B. Column Cards
    grid_cols = st.columns(3) # create a grid layout for cards
    
    for i, col in enumerate(df.columns):
        # Distribute cards across 3 columns
        with grid_cols[i % 3]:
            current_type, sample_val = get_column_info(df, col)
            
            with st.container(border=True):
                st.markdown(f"**{col}**")
                st.caption(f"Type: `{current_type}`")
                st.code(sample_val, language=None)
                
                # Dropdown
                type_options = list(TYPE_MAPPING.keys())
                default_idx = type_options.index(current_type) if current_type in type_options else 0
                new_type = st.selectbox("Convert to:", type_options, index=default_idx, key=f"type_{i}", label_visibility="collapsed")
                
                # Preview & Button
                preview = get_preview_value(sample_val, new_type)
                
                if new_type != current_type:
                    if preview == "null (Cast Failed)":
                        st.error("⚠️ Cast will fail (Null)")
                    else:
                        st.success(f"➝ {preview}")
                    
                    if st.button(f"Apply", key=f"apply_{i}"):
                        save_checkpoint(f"Changed '{col}' to {new_type}") # <--- SAVE HISTORY
                        new_df, err = cast_single_column(df, col, new_type)
                        if err: st.error(err)
                        else:
                            st.session_state["df"] = new_df
                            st.rerun()

    st.markdown("---")
    col_c, col_d = st.columns([4, 1])
    with col_d:
        if st.button("✅ Confirm & Next Step", type="primary", use_container_width=True):
            st.session_state["app_stage"] = "AUDIT"
            st.session_state["schema_locked"] = True
            st.rerun()

# ==============================================================================
# 3. STAGE: AUDIT & CLEAN
# ==============================================================================
elif st.session_state["app_stage"] == "AUDIT":
    st.title("🕵️ Step 3: AI Audit")
    
    # Header & Tools
    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    c1.metric("Rows", st.session_state["df"].height)
    c2.metric("Columns", st.session_state["df"].width)
    if c4.button("🔙 Back to Schema"):
        st.session_state["app_stage"] = "SCHEMA"
        st.rerun()

    df = st.session_state["df"]
    
    tab1, tab2 = st.tabs(["⚡ Structural Scan (Fast)", "🧠 Semantic Scan (Deep)"])

    # --- TAB 1: FAST SCAN ---
    with tab1:
        if st.button("Run Fast Scan"):
            st.session_state["fast_issues"] = scan_structural_issues(df)
            st.rerun()
            
        for i, issue in enumerate(st.session_state["fast_issues"]):
            with st.expander(f"🔴 {issue['type']} in {issue['column']} ({issue['count']} rows)"):
                
                # Menu Logic
                base_options = issue.get('options', [])
                custom_opt = None
                if issue['type'] == "Missing Values": custom_opt = "Fill with Custom Value"
                elif issue['type'] == "Negative Values": custom_opt = "Replace Negatives with Custom Value"
                final_menu = ["Select Action..."] + base_options
                if custom_opt: final_menu.append(custom_opt)

                selected_fix = st.selectbox("Strategy:", final_menu, key=f"fix_sel_{i}")
                
                custom_input_val = None
                if "Custom Value" in selected_fix:
                    custom_input_val = st.text_input(f"Value:", key=f"cust_val_{i}")

                b1, b2 = st.columns([1, 5])
                with b1:
                    if st.button("Ignore", key=f"ign_{i}"):
                        st.session_state["fast_issues"].pop(i)
                        st.rerun()
                with b2:
                    if selected_fix != "Select Action...":
                        if st.button("Apply Fix", type="primary", key=f"appl_{i}"):
                            save_checkpoint(f"Fixed {issue['type']} in {issue['column']}") # <--- SAVE HISTORY
                            
                            with st.spinner("Applying..."):
                                try:
                                    new_df = apply_fix(df, selected_fix, issue['column'], custom_val=custom_input_val)
                                    st.session_state["df"] = new_df
                                    st.session_state["fast_issues"] = scan_structural_issues(new_df)
                                    st.success("Done!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

    # --- TAB 2: DEEP SCAN ---
    with tab2:
        col_scan_1, col_scan_2 = st.columns([1, 3])
        limit = col_scan_1.number_input("Rows to Scan", 100, 5000, 100)
        
        if col_scan_2.button("Start AI Logic Scan"):
            progress = st.progress(0)
            status = st.empty()
            raw_issues = []
            batches = list(get_batches(df.head(limit), 20))
            for i, batch in enumerate(batches):
                progress.progress((i+1)/len(batches))
                status.caption(f"Scanning Batch {i+1}/{len(batches)}...")
                raw_issues.extend(analyze_batch(batch))
            st.session_state["deep_issues"] = aggregate_deep_issues(raw_issues)
            status.success("Complete!")
            st.rerun()

        if st.session_state["deep_issues"]:
            if st.button("Clear Results"): st.session_state["deep_issues"] = []

        for i, issue in enumerate(st.session_state["deep_issues"]):
            with st.expander(f"⚠️ {issue['issue']} in {issue['column']} (Found {issue['count']} times)"):
                st.write(f"Sample Rows: {issue['rows']}")
                if st.button("Fix Logic", key=f"deep_fix_{i}"):
                    save_checkpoint(f"AI Fix: {issue['issue']}") # <--- SAVE HISTORY
                    
                    with st.spinner("Janitor Working..."):
                        profile = get_data_profile(df)
                        code = run_janitor(profile, issue['issue'], f"Fix: {issue['issue']}")
                        try:
                            loc = {'pl': pl}
                            exec(code, globals(), loc)
                            st.session_state["df"] = loc['clean_data'](df)
                            st.success("Fixed!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")

    st.markdown("---")
    st.subheader("📊 Live Data Preview")
    st.dataframe(df.head(50).to_pandas(), use_container_width=True)