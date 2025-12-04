import streamlit as st
import time
import polars as pl
import pandas as pd
from app.deep_scanner import scan_vocabulary_issues, scan_statistical_issues
from app.utils import get_data_profile
from app.agents.janitor import run_janitor
from app.state_manager import save_checkpoint

def get_explanation(issue_type):
    """Returns a human-readable explanation for why this was flagged."""
    explanations = {
        "Potential Typos": "These values appear very rarely (<1%) but look 85%+ similar to common values. They are likely spelling errors.",
        "Inconsistent Case": "The same word appears in multiple variations (e.g., 'ny', 'NY', 'Ny'). This causes grouping errors.",
        "Encoding Artifacts (Mojibake)": "These strings contain garbage characters (e.g., Ã©) often caused by UTF-8 decoding issues.",
        "Invalid Email Format (Missing @)": "These rows are marked as Emails but lack the '@' symbol required for valid addresses.",
        "Statistical Outliers (IQR)": "These numeric values fall far outside the normal range (Interquartile Range). They are statistically extreme compared to the rest of the column.",
        "Suspicious Year (Outside 1900-2030)": "These dates have years that are likely errors (e.g. before 1900 or far in the future)."
    }
    return explanations.get(issue_type, "This pattern was flagged as a potential data quality issue.")

def render_advanced_tab(df):
    """
    Renders Tab 2: Code-based Deep Scans (Vocabulary & Statistics).
    """
    st.info("🔎 **Advanced Scan**: Uses statistical analysis and fuzzy logic to find outliers and typos. Fast & Deterministic.")

    # ==========================================
    # ZONE 1: VOCABULARY SCANNER
    # ==========================================
    with st.expander("🔤 Vocabulary & Text Health", expanded=True):
        st.caption("Checks for Typos, Case Inconsistency, Encoding Errors, and Email Formats.")
        
        # 1. Column Selector (String Columns Only)
        str_cols = df.select(pl.col(pl.String)).columns
        selected_vocab_cols = st.multiselect(
            "Select Columns to Scan:",
            options=str_cols,
            default=str_cols,
            key="vocab_cols",
            help="Uncheck columns like IDs or UUIDs to speed up processing."
        )
        
        if st.button("Run Vocabulary Scan", use_container_width=True):
            if not selected_vocab_cols:
                st.warning("Please select at least one column.")
            else:
                with st.spinner("Scanning text columns..."):
                    start_t = time.time()
                    subset_df = df.select(selected_vocab_cols)
                    issues = scan_vocabulary_issues(subset_df)
                    dur = f"{time.time() - start_t:.2f}s"
                    
                    st.session_state["vocab_issues"] = issues
                    save_checkpoint("Vocabulary Scan", f"Found {len(issues)} issues.", "Audit", "Scan", changeset=f"Time: {dur}")
                    st.rerun()
            
        if st.session_state.get("vocab_issues"):
            st.info(f"Found {len(st.session_state['vocab_issues'])} text issues.")
            
            for i, issue in enumerate(st.session_state["vocab_issues"]):
                st.markdown(f"#### **{issue['column']}**")
                
                # Layout: 3 columns (Description, Evidence, Action)
                c1, c2 = st.columns([2, 1])
                
                with c1:
                    st.error(f"🔴 **Issue:** {issue['type']}")
                    # NEW: Explicit Explanation
                    st.caption(f"**Why was this flagged?**\n{get_explanation(issue['type'])}")
                    st.info(f"💡 **Suggestion:** {issue['suggestion']}")

                with c2:
                    st.metric("Rows Affected", issue['count'])
                    
                    # NEW: Show Flagged Data as a Table
                    with st.expander("View Flagged Data"):
                        # Convert list of examples to DataFrame for nice display
                        ex_df = pd.DataFrame(issue['examples'], columns=["Flagged Values"])
                        st.dataframe(ex_df, hide_index=True, use_container_width=True)

                    if st.button(f"Fix Issue", key=f"vocab_{i}"):
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
                                save_checkpoint(f"Fixed {issue['column']}", f"Fixed {issue['type']}", "Audit", "Structural", changeset)
                                st.success("Fixed!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed: {e}")
                st.divider()
        elif "vocab_issues" in st.session_state and not st.session_state["vocab_issues"]:
             st.success("No vocabulary issues found in selected columns.")

    # ==========================================
    # ZONE 2: STATISTICAL SCANNER
    # ==========================================
    with st.expander("📊 Statistical & Distribution Health", expanded=False):
        st.caption("Checks for numeric outliers (IQR) and suspicious date ranges.")
        
        # 1. Column Selector
        num_date_cols = df.select(pl.col([pl.Int64, pl.Float64, pl.Int32, pl.Float32, pl.Date, pl.Datetime])).columns
        selected_stat_cols = st.multiselect(
            "Select Columns to Scan:",
            options=num_date_cols,
            default=num_date_cols,
            key="stat_cols"
        )
        
        if st.button("Run Statistical Scan", use_container_width=True):
            if not selected_stat_cols:
                st.warning("Please select at least one column.")
            else:
                with st.spinner("Calculating statistics..."):
                    start_t = time.time()
                    subset_df = df.select(selected_stat_cols)
                    issues = scan_statistical_issues(subset_df)
                    dur = f"{time.time() - start_t:.2f}s"
                    
                    st.session_state["stat_issues"] = issues
                    save_checkpoint("Statistical Scan", f"Found {len(issues)} anomalies.", "Audit", "Scan", changeset=f"Time: {dur}")
                    st.rerun()
            
        if st.session_state.get("stat_issues"):
            st.info(f"Found {len(st.session_state['stat_issues'])} anomalies.")
            
            for i, issue in enumerate(st.session_state["stat_issues"]):
                st.markdown(f"#### **{issue['column']}**")
                
                c1, c2 = st.columns([2, 1])
                with c1:
                    st.error(f"🔴 **Issue:** {issue['type']}")
                    # NEW: Explicit Explanation
                    st.caption(f"**Why was this flagged?**\n{get_explanation(issue['type'])}")
                    st.info(f"💡 **Suggestion:** {issue['suggestion']}")
                
                with c2:
                    st.metric("Rows Affected", issue['count'])
                    
                    # NEW: Show Flagged Data as a Table
                    with st.expander("View Outlier Values"):
                        ex_df = pd.DataFrame(issue['examples'], columns=["Outlier Values"])
                        st.dataframe(ex_df, hide_index=True, use_container_width=True)

                    if st.button(f"Fix Issue", key=f"stat_{i}"):
                        with st.spinner("Janitor capping/fixing stats..."):
                            profile = get_data_profile(df)
                            prompt = f"Fix {issue['type']} in '{issue['column']}'. {issue['suggestion']}."
                            code = run_janitor(profile, issue['type'], prompt)
                            
                            try:
                                loc = {'pl': pl}
                                exec(code, globals(), loc)
                                new_df = loc['clean_data'](df)
                                st.session_state["df"] = new_df
                                
                                diff = df.height - new_df.height
                                changeset = f"Dropped {diff} rows" if diff > 0 else "Values Modified"

                                save_checkpoint(f"Fixed {issue['column']}", f"Fixed {issue['type']}", "Audit", "Structural", changeset)
                                st.success("Fixed!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed: {e}")
                st.divider()
        elif "stat_issues" in st.session_state and not st.session_state["stat_issues"]:
             st.success("No statistical anomalies found in selected columns.")