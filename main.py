import streamlit as st
import polars as pl
from datetime import datetime

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

# --- HELPER: INPUT VALIDATION ---
def validate_input(val, dtype_str):
    if not val: return True, val 
    is_numeric_col = "Int" in dtype_str or "Float" in dtype_str
    if is_numeric_col:
        try:
            float(val)
            return True, val
        except ValueError:
            return False, None
    return True, val

def run_fast_scan_filtered(df):
    raw_issues = scan_structural_issues(df)
    filtered_issues = []
    if "ignored_issues" not in st.session_state:
        st.session_state["ignored_issues"] = set()
    for issue in raw_issues:
        issue_sig = (issue['column'], issue['type'])
        if issue_sig not in st.session_state["ignored_issues"]:
            filtered_issues.append(issue)
    return filtered_issues

# ==============================================================================
# 🕒 SIDEBAR: INTELLIGENT AUTO-EXPAND HISTORY
# ==============================================================================
with st.sidebar:
    st.title("📜 Audit Trail")
    
    history = st.session_state["history"]
    if not history:
        st.info("No actions recorded yet.")
    
    # --- DETERMINE LATEST ACTION CONTEXT ---
    # We use this to auto-expand the relevant sub-folder
    last_event = history[-1] if history else None
    last_cat = last_event['category'] if last_event else None
    last_sub = last_event['sub_category'] if last_event else None

    def render_history_item(i, event):
        is_latest = (i == len(history) - 1)
        btn_label = "↩️" if is_latest else "⏪"
        help_text = "Undo this change" if is_latest else "⚠️ Reverting here undoes subsequent actions."
        
        c1, c2 = st.columns([5, 1])
        with c1:
            st.markdown(f"**{event['summary']}**")
            if event['details']: st.caption(f"{event['details']}")
            st.caption(f"_{event['timestamp']}_")
        with c2:
            if st.button(btn_label, key=f"hist_{i}", help=help_text):
                restore_checkpoint(i-1 if i > 0 else 0)
                st.rerun()
        st.divider()

    # --- STEP 3: AUDIT ---
    audit_events = [(i, e) for i, e in enumerate(history) if e["category"] == "Audit"]
    if audit_events:
        # Expand Main Step if Active
        is_step_active = st.session_state["app_stage"] == "AUDIT"
        with st.expander("Step 3: AI Audit", expanded=is_step_active):
            
            # Sub-Categories
            structural = [x for x in audit_events if x[1].get('sub_category') == 'Structural']
            logic = [x for x in audit_events if x[1].get('sub_category') == 'Logic']
            scans = [x for x in audit_events if x[1].get('sub_category') == 'Scan']
            system = [x for x in audit_events if x[1].get('sub_category') == 'System']

            # Dynamic Expansion Logic: Open if it matches the LAST action
            expand_struct = (last_cat == "Audit" and last_sub == "Structural")
            expand_logic = (last_cat == "Audit" and last_sub == "Logic")
            expand_scan = (last_cat == "Audit" and last_sub == "Scan")
            
            if structural:
                with st.expander(f"⚡ Structural Fixes ({len(structural)})", expanded=expand_struct):
                    for i, event in reversed(structural): render_history_item(i, event)

            if logic:
                with st.expander(f"🧠 Logic Fixes ({len(logic)})", expanded=expand_logic):
                    for i, event in reversed(logic): render_history_item(i, event)

            if scans:
                with st.expander(f"🔍 Scan Runs ({len(scans)})", expanded=expand_scan):
                    for i, event in reversed(scans): render_history_item(i, event)
            
            if system:
                with st.expander(f"ℹ️ System Logs ({len(system)})", expanded=False):
                    for i, event in reversed(system): render_history_item(i, event)

    # --- STEP 2: SCHEMA ---
    schema_events = [(i, e) for i, e in enumerate(history) if e["category"] == "Schema"]
    if schema_events:
        is_step_active = st.session_state["app_stage"] == "SCHEMA"
        with st.expander("Step 2: Schema Validation", expanded=is_step_active):
            
            updates = [x for x in schema_events if x[1].get('sub_category') == 'Schema Updates']
            system = [x for x in schema_events if x[1].get('sub_category') == 'System']
            
            # Expand 'Updates' if we are currently editing schema
            expand_updates = (last_cat == "Schema" and last_sub == "Schema Updates")

            if updates:
                with st.expander(f"✏️ Schema Updates ({len(updates)})", expanded=expand_updates):
                    for i, event in reversed(updates): render_history_item(i, event)

            if system:
                with st.expander(f"ℹ️ System Logs ({len(system)})", expanded=False):
                    for i, event in reversed(system): render_history_item(i, event)

    # --- STEP 1: INGESTION ---
    ingestion_events = [(i, e) for i, e in enumerate(history) if e["category"] == "Ingestion"]
    if ingestion_events:
        is_step_active = st.session_state["app_stage"] == "UPLOAD"
        with st.expander("Step 1: Data Ingestion", expanded=is_step_active):
            for i, event in reversed(ingestion_events): render_history_item(i, event)

    if history and st.button("♻️ Reset Project", use_container_width=True):
        st.session_state.clear()
        st.rerun()

# ==============================================================================
# 1. STAGE: UPLOAD
# ==============================================================================
if st.session_state["app_stage"] == "UPLOAD":
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
                save_checkpoint("Data Loaded", f"File: {uploaded_file.name} | Rows: {df.height}", "Ingestion")
                st.session_state["app_stage"] = "SCHEMA"
                st.rerun()

# ==============================================================================
# 2. STAGE: SCHEMA MANAGER
# ==============================================================================
elif st.session_state["app_stage"] == "SCHEMA":
    st.title("🛠️ Step 2: Schema Validation")
    
    df = st.session_state["df"]
    
    if not st.session_state["schema_locked"]:
        st.info("Review and correct data types below.")
        
        with st.expander("📄 View Initial Detected Schema", expanded=False):
            st.dataframe(st.session_state["initial_schema_view"], use_container_width=True, hide_index=True)

        st.subheader("Modify Columns")
        for i, col in enumerate(df.columns):
            current_type, sample_val = get_column_info(df, col)
            with st.expander(f"**{col}** ({current_type})"):
                c1, c2, c3 = st.columns([2, 2, 1])
                with c1:
                    st.caption("Sample")
                    st.code(sample_val)
                with c2:
                    type_options = list(TYPE_MAPPING.keys())
                    default_idx = type_options.index(current_type) if current_type in type_options else 0
                    new_type = st.selectbox("Convert to:", type_options, index=default_idx, key=f"type_{i}")
                    preview = get_preview_value(sample_val, new_type)
                    st.caption(f"Preview: {preview}")
                with c3:
                    st.write("")
                    st.write("")
                    if st.button("Update", key=f"upd_{i}"):
                        if new_type != current_type:
                            new_df, err = cast_single_column(df, col, new_type)
                            if err: st.error(err)
                            else:
                                st.session_state["df"] = new_df
                                save_checkpoint(f"Updated '{col}'", f"Changed to {new_type}", "Schema", "Schema Updates")
                                st.rerun()

        st.markdown("---")
        
        st.subheader("Current Schema Snapshot")
        current_view = get_current_schema_view(df)
        st.dataframe(current_view, use_container_width=True, hide_index=True)
        
        if st.button("✅ Confirm Schema & Proceed", type="primary"):
            save_checkpoint("Schema Confirmed", "Locked data types.", "Schema", "System")
            st.session_state["schema_locked"] = True
            st.session_state["app_stage"] = "AUDIT"
            st.rerun()

# ==============================================================================
# 3. STAGE: AUDIT & CLEAN
# ==============================================================================
elif st.session_state["app_stage"] == "AUDIT":
    st.title("🕵️ Step 3: AI Audit")
    
    df = st.session_state["df"]
    
    struct_current = len(st.session_state["fast_issues"])
    logic_current = sum([i['count'] for i in st.session_state["deep_issues"]]) if st.session_state["deep_issues"] else 0
    
    diff_struct = struct_current - st.session_state.get("fast_count_start", 0)
    diff_logic = logic_current - st.session_state.get("deep_count_start", 0)

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

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Rows", df.height)
    m2.metric("Columns", df.width)
    m3.metric("Structural Issues", struct_current, delta=f"{diff_struct}" if diff_struct != 0 else None, delta_color="inverse")
    m4.metric("Logic Issues", logic_current, delta=f"{diff_logic}" if diff_logic != 0 else None, delta_color="inverse")

    st.divider()
    
    tab1, tab2 = st.tabs(["⚡ Fast Scan (Structural)", "🧠 Deep Scan (Logic)"])

    # --- FAST SCAN ---
    with tab1:
        if st.button("Run Fast Scan"):
            filtered = run_fast_scan_filtered(df)
            st.session_state["fast_issues"] = filtered
            
            if st.session_state["fast_count_start"] == 0:
                st.session_state["fast_count_start"] = len(filtered)
            
            save_checkpoint("Fast Scan Run", f"Found {len(filtered)} issues.", "Audit", "Scan")
            st.rerun()
            
        for i, issue in enumerate(st.session_state["fast_issues"]):
            with st.expander(f"🔴 {issue['type']} in {issue['column']} ({issue['count']} rows)"):
                
                base_options = issue.get('options', [])
                custom_opt = None
                if issue['type'] == "Missing Values": custom_opt = "Fill with Custom Value"
                elif issue['type'] == "Negative Values": custom_opt = "Replace Negatives with Custom Value"
                
                final_menu = ["Select Action..."] + base_options
                if custom_opt: final_menu.append(custom_opt)

                unique_key = f"{issue['column']}_{issue['type']}"
                selected_fix = st.selectbox("Strategy:", final_menu, key=f"fix_{unique_key}")
                
                custom_input_val = None
                if "Custom Value" in selected_fix:
                    custom_input_val = st.text_input(f"Value:", key=f"val_{unique_key}")

                b1, b2 = st.columns([1, 5])
                with b1:
                    if st.button("🚫 Ignore", key=f"ign_{unique_key}"):
                        st.session_state["ignored_issues"].add((issue['column'], issue['type']))
                        save_checkpoint(f"Ignored Issue", f"Ignored {issue['type']} in {issue['column']}", "Audit", "Structural")
                        st.session_state["fast_issues"].pop(i)
                        st.rerun()
                with b2:
                    if selected_fix != "Select Action...":
                        if st.button("Apply Fix", type="primary", key=f"appl_{unique_key}"):
                            valid = True
                            final_val = custom_input_val
                            if "Custom Value" in selected_fix:
                                valid, final_val = validate_input(custom_input_val, issue.get('dtype', ''))
                                if not valid: st.error("Invalid Input")
                            
                            if valid:
                                with st.spinner("Applying..."):
                                    try:
                                        clean_strategy = selected_fix
                                        if ": " in clean_strategy:
                                            clean_strategy = clean_strategy.split(": ", 1)[1]
                                        
                                        new_df = apply_fix(df, clean_strategy, issue['column'], custom_val=final_val)
                                        
                                        st.session_state["df"] = new_df
                                        st.session_state["fast_issues"] = run_fast_scan_filtered(new_df)
                                        
                                        fix_desc = f"Strategy: {clean_strategy}"
                                        if custom_input_val: fix_desc += f" ({custom_input_val})"
                                        
                                        save_checkpoint(
                                            f"Fixed {issue['column']}", 
                                            fix_desc, 
                                            "Audit", 
                                            "Structural"
                                        )
                                        st.success("Done!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")

    # --- DEEP SCAN ---
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
            
            issues = aggregate_deep_issues(raw_issues)
            st.session_state["deep_issues"] = issues
            
            if st.session_state["deep_count_start"] == 0:
                st.session_state["deep_count_start"] = sum([x['count'] for x in issues])
            
            save_checkpoint("Logic Scan Run", f"Found {len(issues)} anomalies.", "Audit", "Scan")
            status.success("Complete!")
            st.rerun()

        if st.session_state["deep_issues"]:
            if st.button("Clear Results"): st.session_state["deep_issues"] = []

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
                            
                            save_checkpoint(
                                f"AI Fix: {issue['column']}", 
                                f"Logic Fixed: {issue['issue']}", 
                                "Audit", 
                                "Logic"
                            )
                            st.success("Fixed!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")

    st.markdown("---")
    st.subheader("📊 Live Data Preview")
    st.dataframe(df.head(50).to_pandas(), use_container_width=True)