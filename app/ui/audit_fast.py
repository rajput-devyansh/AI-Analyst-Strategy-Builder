import streamlit as st
import time
from app.scanner import scan_structural_issues
from app.structural_fixer import apply_fix 
from app.state_manager import save_checkpoint
from app.utils import validate_input

def run_fast_scan_filtered(df):
    """Internal helper to filter ignored issues."""
    raw_issues = scan_structural_issues(df)
    filtered_issues = []
    
    if "ignored_issues" not in st.session_state:
        st.session_state["ignored_issues"] = set()
        
    for issue in raw_issues:
        issue_sig = (issue['column'], issue['type'])
        if issue_sig not in st.session_state["ignored_issues"]:
            filtered_issues.append(issue)
    return filtered_issues

def render_fast_scan_tab(df):
    """Renders the Structural Scan UI."""
    
    if st.button("Run Fast Scan", use_container_width=True):
        # 1. Capture Start Time
        start_t = time.time()
        
        filtered = run_fast_scan_filtered(df)
        st.session_state["fast_issues"] = filtered
        
        # 2. Calculate Duration
        dur = f"{time.time() - start_t:.2f}s"
        
        # Set Baseline if first run
        if st.session_state["fast_count_start"] == 0:
            st.session_state["fast_count_start"] = len(filtered)
        
        # 3. Save Checkpoint with Time
        save_checkpoint(
            "Fast Scan Run", 
            f"Found {len(filtered)} issues.", 
            "Audit", "Scan", 
            changeset=f"Time: {dur}"
        )
        st.rerun()
        
    if not st.session_state["fast_issues"]:
        st.info("No structural issues detected (or scan not run yet).")

    # Render Cards
    for i, issue in enumerate(st.session_state["fast_issues"]):
        with st.expander(f"🔴 {issue['type']} in {issue['column']} ({issue['count']} rows)"):
            
            # 1. Menu Construction
            base_options = issue.get('options', [])
            custom_opt = None
            if issue['type'] == "Missing Values": custom_opt = "Fill with Custom Value"
            elif issue['type'] == "Negative Values": custom_opt = "Replace Negatives with Custom Value"
            
            final_menu = ["Select Action..."] + base_options
            if custom_opt: final_menu.append(custom_opt)

            # Unique Key
            unique_key = f"{issue['column']}_{issue['type']}"
            selected_fix = st.selectbox("Strategy:", final_menu, key=f"fix_{unique_key}")
            
            # 2. Custom Input
            custom_input_val = None
            if "Custom Value" in selected_fix:
                col_dtype = issue.get('dtype', 'Unknown')
                st.caption(f"Column Type: **{col_dtype}**")
                custom_input_val = st.text_input(f"Value:", key=f"val_{unique_key}")

            # 3. Actions
            b1, b2 = st.columns([1, 5])
            with b1:
                # IGNORE BUTTON
                if st.button("🚫 Ignore", key=f"ign_{unique_key}"):
                    st.session_state["ignored_issues"].add((issue['column'], issue['type']))
                    
                    save_checkpoint(
                        f"Ignored Issue", 
                        f"Ignored {issue['type']} in {issue['column']}", 
                        "Audit", "Structural"
                    )
                    st.session_state["fast_issues"].pop(i)
                    st.rerun()
            
            with b2:
                # APPLY BUTTON
                if selected_fix != "Select Action...":
                    if st.button("Apply Fix", type="primary", key=f"appl_{unique_key}"):
                        
                        # Validate
                        valid = True
                        final_val = custom_input_val
                        if "Custom Value" in selected_fix:
                            valid, final_val = validate_input(custom_input_val, issue.get('dtype', ''))
                            if not valid: st.error("Invalid Input Format.")
                        
                        if valid:
                            with st.spinner("Applying..."):
                                try:
                                    clean_strategy = selected_fix
                                    if ": " in clean_strategy:
                                        clean_strategy = clean_strategy.split(": ", 1)[1]
                                    
                                    new_df = apply_fix(df, clean_strategy, issue['column'], custom_val=final_val)
                                    
                                    # Calculate Impact (Rows Dropped)
                                    rows_before = df.height
                                    rows_after = new_df.height
                                    diff = rows_before - rows_after
                                    change_msg = f"Dropped {diff} rows" if diff > 0 else "Values Updated"

                                    st.session_state["df"] = new_df
                                    st.session_state["fast_issues"] = run_fast_scan_filtered(new_df)
                                    
                                    fix_desc = f"Strategy: {clean_strategy}"
                                    if custom_input_val: fix_desc += f" ({custom_input_val})"
                                    
                                    save_checkpoint(
                                        f"Fixed {issue['column']}", 
                                        fix_desc, 
                                        "Audit", "Structural", 
                                        changeset=change_msg
                                    )
                                    st.success("Done!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")