import streamlit as st
import polars as pl
from app.utils import load_data, get_data_profile
from app.scanner import scan_structural_issues
from app.deep_scanner import get_batches, analyze_batch, aggregate_deep_issues
from app.agents.janitor import run_janitor
from app.schema_manager import get_schema_summary, apply_schema_changes, TYPE_MAPPING

st.set_page_config(page_title="AI Data Auditor", layout="wide")
st.title("🧠 AI Analyst: Dual-Engine Audit")

# --- STATE MANAGEMENT ---
if "df" not in st.session_state: st.session_state["df"] = None
if "schema_locked" not in st.session_state: st.session_state["schema_locked"] = False
if "fast_issues" not in st.session_state: st.session_state["fast_issues"] = []
if "deep_issues" not in st.session_state: st.session_state["deep_issues"] = []

# --- SIDEBAR: INGESTION ---
with st.sidebar:
    st.header("1. Ingestion")
    uploaded_file = st.file_uploader("Upload Chaos Data", type=["csv", "xlsx"])
    if uploaded_file and st.button("Load Data"):
        df = load_data(uploaded_file)
        if isinstance(df, str):
            st.error(df)
        else:
            # Reset state on new load
            st.session_state["df"] = df
            st.session_state["schema_locked"] = False
            st.session_state["fast_issues"] = []
            st.session_state["deep_issues"] = []
            # Clear editor state if exists
            if "schema_editor" in st.session_state:
                del st.session_state["schema_editor"]
            st.success(f"Loaded {df.height} rows.")
            st.rerun()

# --- MAIN EXECUTION ---
if st.session_state["df"] is not None:
    df = st.session_state["df"]
    
    # ==================================================
    # PHASE 0: SCHEMA VALIDATION (The "Quality Gate")
    # ==================================================
    if not st.session_state["schema_locked"]:
        st.info("👇 Validate data types. The 'Updated Format' column previews your changes.")
        
        # 1. Get current edits if they exist (to generate live previews)
        current_edits = None
        if "schema_editor" in st.session_state:
            current_edits = st.session_state["schema_editor"]
            
        # 2. Generate the Summary Table with Previews
        schema_df = get_schema_summary(df, current_edits)
        
        # 3. Show Editable Table
        edited_schema = st.data_editor(
            schema_df,
            key="schema_editor", 
            column_config={
                "Column Name": st.column_config.Column(disabled=True),
                "Detected Type": st.column_config.Column(disabled=True),
                
                # VISUAL: Raw Data
                "Actual Raw Format": st.column_config.TextColumn(
                    "Actual Raw Format", 
                    disabled=True, 
                    help="How the data looks right now (Sample)"
                ),
                
                # INTERACTIVE: Dropdown
                "Target Type": st.column_config.SelectboxColumn(
                    "Change Data Type",
                    help="Select new type",
                    width="medium",
                    options=list(TYPE_MAPPING.keys()),
                    required=True
                ),
                
                # VISUAL: Preview
                "Updated Format": st.column_config.TextColumn(
                    "Updated Format (Preview)", 
                    disabled=True,
                    help="What the data will become after casting"
                )
            },
            hide_index=True,
            use_container_width=True
        )
        
        # 4. Save Button
        if st.button("✅ Confirm Data Types & Proceed", type="primary"):
            with st.spinner("Applying strict type casting..."):
                new_df, errors = apply_schema_changes(df, edited_schema)
                
                if errors:
                    st.error("Errors during casting:")
                    for e in errors: st.write(e)
                else:
                    st.session_state["df"] = new_df
                    st.session_state["schema_locked"] = True
                    st.success("Schema Applied Successfully!")
                    st.rerun()
                    
    # ==================================================
    # PHASE 1 & 2: THE ANALYST (Only after locking schema)
    # ==================================================
    else:
        # Show a small summary of the locked schema
        with st.expander("✅ Data Types Verified (Click to Re-open)", expanded=False):
            if st.button("Reset Schema"):
                st.session_state["schema_locked"] = False
                st.rerun()
            st.dataframe(df.head(3).to_pandas())

        # METRICS
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", df.height)
        c2.metric("Structural Issues", len(st.session_state["fast_issues"]))
        c3.metric("Logic Issues", len(st.session_state["deep_issues"]))

        # --- TABS DEFINITION ---
        tab1, tab2 = st.tabs(["⚡ Fast Scan (Structure)", "🧠 Deep Scan (Logic)"])

        # === TAB 1: FAST SCAN ===
        with tab1:
            if st.button("Run Fast Scan"):
                st.session_state["fast_issues"] = scan_structural_issues(df)
                st.rerun()
                
            if not st.session_state["fast_issues"]:
                st.info("No structural issues detected (or scan not run yet).")

            for i, issue in enumerate(st.session_state["fast_issues"]):
                with st.expander(f"🔴 {issue['type']} in {issue['column']} (Count: {issue['count']})"):
                    
                    # 1. Construct the Menu Options
                    if 'strategies' in issue:
                        rec_fix = f"⭐ Rec: {issue['strategies']['rec']}"
                        alt_fixes = [f"Option: {alt}" for alt in issue['strategies']['alts']]
                        menu_options = [rec_fix] + alt_fixes + ["✏️ Custom Input", "🚫 Ignore"]
                    else:
                        menu_options = ["Auto-Fix", "✏️ Custom Input", "🚫 Ignore"]

                    # 2. Render Selectbox
                    selected_option = st.selectbox(
                        "Choose Strategy:", 
                        menu_options, 
                        key=f"fast_select_{i}"
                    )
                    
                    # 3. Logic for Inputs
                    final_instruction = ""
                    
                    if "Custom" in selected_option:
                        final_instruction = st.text_input("Describe your fix:", key=f"fast_custom_{i}")
                    elif "Ignore" in selected_option:
                        st.caption("Issue will be ignored.")
                    elif "Auto-Fix" in selected_option:
                        final_instruction = issue.get('suggestion', 'Fix this issue')
                    else:
                        # Strip the prefixes ("⭐ Rec: ", "Option: ")
                        if ": " in selected_option:
                            final_instruction = selected_option.split(": ", 1)[1]
                        else:
                            final_instruction = selected_option

                    # 4. Apply Button
                    if "Ignore" not in selected_option:
                        if st.button("Apply Fix", key=f"fast_btn_{i}"):
                            if not final_instruction: 
                                final_instruction = issue.get('suggestion', 'Fix this issue')

                            with st.spinner(f"Janitor applying: {final_instruction}..."):
                                profile = get_data_profile(df)
                                code = run_janitor(profile, issue['type'], final_instruction)
                                
                                try:
                                    loc = {'pl': pl}
                                    exec(code, globals(), loc)
                                    st.session_state["df"] = loc['clean_data'](df)
                                    st.success("Fixed!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed: {e}")

        # === TAB 2: DEEP SCAN ===
        with tab2:
            st.info("AI will read rows in batches to find logic contradictions.")
            col_scan_1, col_scan_2 = st.columns([1, 3])
            limit = col_scan_1.number_input("Rows to Scan", value=100, step=50)
            
            if col_scan_2.button("Start Deep Logic Scan"):
                progress = st.progress(0)
                status = st.empty()
                raw_issues = []
                
                batches = list(get_batches(df.head(limit), 20))
                for i, batch in enumerate(batches):
                    progress.progress((i+1)/len(batches))
                    status.text(f"Scanning Batch {i+1}/{len(batches)}...")
                    raw_issues.extend(analyze_batch(batch))
                    
                st.session_state["deep_issues"] = aggregate_deep_issues(raw_issues)
                status.success("Scan Complete!")
                st.rerun()
                
            if not st.session_state["deep_issues"] and st.button("Clear Deep Scan Results"):
                 st.session_state["deep_issues"] = []

            for i, issue in enumerate(st.session_state["deep_issues"]):
                with st.expander(f"⚠️ {issue['issue']} in {issue['column']} (Found {issue['count']} times)"):
                    st.write(f"Sample Row Indices: {issue['rows']}")
                    
                    if st.button("Fix This Logic", key=f"deep_{issue['column']}_{i}"):
                        with st.spinner("Janitor fixing logic..."):
                            profile = get_data_profile(df)
                            code = run_janitor(profile, issue['issue'], f"Fix this logical error: {issue['issue']}")
                            
                            try:
                                loc = {'pl': pl}
                                exec(code, globals(), loc)
                                st.session_state["df"] = loc['clean_data'](df)
                                st.success("Logic Fixed!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed: {e}")

        # FOOTER PREVIEW
        st.write("---")
        st.subheader("📊 Live Data Preview")
        st.dataframe(df.head(50).to_pandas(), use_container_width=True)