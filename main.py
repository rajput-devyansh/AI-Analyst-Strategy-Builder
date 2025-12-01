import streamlit as st
import polars as pl
from app.utils import load_data, get_data_profile
from app.scanner import scan_structural_issues
from app.deep_scanner import get_batches, analyze_batch, aggregate_deep_issues
from app.agents.janitor import run_janitor

st.set_page_config(page_title="AI Data Auditor", layout="wide")
st.title("🧠 AI Analyst: Dual-Engine Audit")

# --- STATE ---
if "df" not in st.session_state: st.session_state["df"] = None
if "fast_issues" not in st.session_state: st.session_state["fast_issues"] = []
if "deep_issues" not in st.session_state: st.session_state["deep_issues"] = []

# --- SIDEBAR ---
with st.sidebar:
    st.header("1. Ingestion")
    uploaded_file = st.file_uploader("Upload Chaos Data", type=["csv", "xlsx"])
    if uploaded_file and st.button("Load Data"):
        df = load_data(uploaded_file)
        if isinstance(df, str):
            st.error(df)
        else:
            st.session_state["df"] = df
            st.session_state["fast_issues"] = []
            st.session_state["deep_issues"] = []
            st.success(f"Loaded {df.height} rows.")
            st.rerun()

# --- MAIN ---
if st.session_state["df"] is not None:
    df = st.session_state["df"]
    
    # METRICS
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", df.height)
    c2.metric("Structural Issues", len(st.session_state["fast_issues"]))
    c3.metric("Logic Issues", len(st.session_state["deep_issues"]))

    tab1, tab2 = st.tabs(["⚡ Fast Scan (Structure)", "🧠 Deep Scan (Logic)"])

    # === TAB 1: FAST SCAN ===
    with tab1:
        if st.button("Run Fast Scan"):
            st.session_state["fast_issues"] = scan_structural_issues(df)
            st.rerun()
            
        for issue in st.session_state["fast_issues"]:
            with st.expander(f"🔴 {issue['type']} in {issue['column']} (Count: {issue['count']})"):
                action = st.selectbox("Fix Strategy:", ["Auto-Fix", "Custom"], key=f"fast_{issue['column']}")
                custom_txt = st.text_input("Custom Logic:", key=f"txt_{issue['column']}") if action == "Custom" else ""
                
                if st.button("Apply Fix", key=f"btn_{issue['column']}"):
                    instr = custom_txt if action == "Custom" else issue['suggestion']
                    
                    with st.spinner("Janitor Working..."):
                        profile = get_data_profile(df)
                        code = run_janitor(profile, issue['type'], instr)
                        
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
            
            # Batch Processing
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

        # Fix: Enumerate to ensure unique keys
        for i, issue in enumerate(st.session_state["deep_issues"]):
            with st.expander(f"⚠️ {issue['issue']} in {issue['column']} (Found {issue['count']} times)"):
                st.write(f"Sample Row Indices: {issue['rows']}")
                
                # Unique Key: key=f"deep_{issue['column']}_{i}"
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

    # PREVIEW (Fixed Width for Streamlit 2025)
    st.write("---")
    st.dataframe(df.head(50).to_pandas(), width="stretch")