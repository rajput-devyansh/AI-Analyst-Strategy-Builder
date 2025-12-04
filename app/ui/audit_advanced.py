import streamlit as st
import time
import polars as pl
from app.deep_scanner import scan_vocabulary_issues, scan_statistical_issues
from app.utils import get_data_profile
from app.agents.janitor import run_janitor
from app.state_manager import save_checkpoint

def render_advanced_tab(df):
    """
    Renders Tab 2: Code-based Deep Scans (Vocabulary & Statistics).
    """
    st.info("🔎 **Advanced Scan**: Uses statistical analysis and fuzzy logic to find outliers and typos. Fast & Deterministic.")

    # --- ZONE 1: VOCABULARY SCANNER ---
    with st.expander("🔤 Vocabulary & Text Health", expanded=True):
        st.caption("Checks for Typos, Case Inconsistency, Encoding Errors, and Email Formats.")
        
        if st.button("Run Vocabulary Scan", use_container_width=True):
            with st.spinner("Scanning text columns..."):
                start_t = time.time()
                issues = scan_vocabulary_issues(df)
                dur = f"{time.time() - start_t:.2f}s"
                
                st.session_state["vocab_issues"] = issues
                # SAVE TIME HERE
                save_checkpoint(
                    "Vocabulary Scan", 
                    f"Found {len(issues)} issues.", 
                    "Audit", "Scan", 
                    changeset=f"Time: {dur}"
                )
                st.rerun()
            
        if st.session_state["vocab_issues"]:
            st.info(f"Found {len(st.session_state['vocab_issues'])} text issues.")
            
            for i, issue in enumerate(st.session_state["vocab_issues"]):
                st.markdown(f"**{issue['column']}**: {issue['type']} ({issue['count']} rows)")
                st.caption(f"Examples: {issue['examples']}")
                
                if st.button(f"Fix {issue['type']}", key=f"vocab_{i}"):
                    with st.spinner("Janitor fixing text..."):
                        profile = get_data_profile(df)
                        prompt = f"Fix {issue['type']} in column '{issue['column']}'. Suggestion: {issue['suggestion']}."
                        code = run_janitor(profile, issue['type'], prompt)
                        
                        try:
                            loc = {'pl': pl}
                            exec(code, globals(), loc)
                            new_df = loc['clean_data'](df)
                            
                            diff = df.height - new_df.height
                            changeset = f"Dropped {diff} rows" if diff > 0 else "Values Updated"
                            
                            st.session_state["df"] = new_df
                            save_checkpoint(
                                f"Fixed {issue['column']}", 
                                f"Fixed {issue['type']}", 
                                "Audit", "Structural", 
                                changeset
                            )
                            st.success("Fixed!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")
                st.divider()
        elif "vocab_issues" in st.session_state and st.session_state["vocab_issues"] == []:
             st.success("No vocabulary issues found.")

    # --- ZONE 2: STATISTICAL SCANNER ---
    with st.expander("📊 Statistical & Distribution Health", expanded=False):
        st.caption("Checks for numeric outliers (IQR) and suspicious date ranges.")
        
        if st.button("Run Statistical Scan", use_container_width=True):
            with st.spinner("Calculating statistics..."):
                start_t = time.time()
                issues = scan_statistical_issues(df)
                dur = f"{time.time() - start_t:.2f}s"
                
                st.session_state["stat_issues"] = issues
                # SAVE TIME HERE
                save_checkpoint(
                    "Statistical Scan", 
                    f"Found {len(issues)} anomalies.", 
                    "Audit", "Scan", 
                    changeset=f"Time: {dur}"
                )
                st.rerun()
            
        if st.session_state["stat_issues"]:
            st.info(f"Found {len(st.session_state['stat_issues'])} anomalies.")
            
            for i, issue in enumerate(st.session_state["stat_issues"]):
                st.markdown(f"**{issue['column']}**: {issue['type']} ({issue['count']} rows)")
                st.caption(f"Examples: {issue['examples']}")
                
                if st.button(f"Fix {issue['type']}", key=f"stat_{i}"):
                    with st.spinner("Janitor capping/fixing stats..."):
                        profile = get_data_profile(df)
                        prompt = f"Fix {issue['type']} in '{issue['column']}'. {issue['suggestion']}."
                        code = run_janitor(profile, issue['type'], prompt)
                        
                        try:
                            loc = {'pl': pl}
                            exec(code, globals(), loc)
                            new_df = loc['clean_data'](df)
                            st.session_state["df"] = new_df
                            
                            # Calculate Diff for stats fix
                            diff = df.height - new_df.height
                            changeset = f"Dropped {diff} rows" if diff > 0 else "Values Modified"

                            save_checkpoint(
                                f"Fixed {issue['column']}", 
                                f"Fixed {issue['type']}", 
                                "Audit", "Structural",
                                changeset
                            )
                            st.success("Fixed!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")
                st.divider()