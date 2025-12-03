import streamlit as st
import polars as pl
import datetime

def init_session_state():
    """Initialize all session variables."""
    if "df" not in st.session_state: st.session_state["df"] = None
    if "history" not in st.session_state: st.session_state["history"] = []
    if "schema_locked" not in st.session_state: st.session_state["schema_locked"] = False
    if "app_stage" not in st.session_state: st.session_state["app_stage"] = "UPLOAD" 
    
    # Issue Tracking
    if "fast_issues" not in st.session_state: st.session_state["fast_issues"] = []
    if "deep_issues" not in st.session_state: st.session_state["deep_issues"] = []
    if "ignored_issues" not in st.session_state: st.session_state["ignored_issues"] = set()
    
    # KPI Baselines
    if "fast_count_start" not in st.session_state: st.session_state["fast_count_start"] = 0
    if "deep_count_start" not in st.session_state: st.session_state["deep_count_start"] = 0

def save_checkpoint(action_summary: str, details: str = None, category: str = "General", sub_category: str = None):
    """
    Saves a snapshot including DATA, SCAN RESULTS, and IGNORED REGISTRY.
    Now includes DE-DUPLICATION logic.
    """
    if st.session_state["df"] is not None:
        
        # --- PREVENT DUPLICATES ---
        if len(st.session_state["history"]) > 0:
            last_entry = st.session_state["history"][-1]
            # If the summary and details are identical to the last action, skip saving
            if last_entry["summary"] == action_summary and last_entry["details"] == details:
                return

        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Clone complex objects
        fast_issues_snapshot = list(st.session_state["fast_issues"])
        deep_issues_snapshot = list(st.session_state["deep_issues"])
        ignored_snapshot = set(st.session_state["ignored_issues"])
        
        snapshot = {
            "timestamp": ts,
            "df": st.session_state["df"].clone(),
            "summary": action_summary,
            "details": details,
            "category": category, 
            "sub_category": sub_category,
            "stage_state": st.session_state["app_stage"],
            "schema_locked": st.session_state["schema_locked"],
            
            "fast_issues": fast_issues_snapshot,
            "deep_issues": deep_issues_snapshot,
            "ignored_issues": ignored_snapshot,
            
            "fast_count_start": st.session_state["fast_count_start"],
            "deep_count_start": st.session_state["deep_count_start"]
        }
        st.session_state["history"].append(snapshot)

def restore_checkpoint(index: int):
    """
    Reverts to a specific point in time.
    """
    snapshot = st.session_state["history"][index]
    
    st.session_state["df"] = snapshot["df"].clone()
    st.session_state["app_stage"] = snapshot["stage_state"]
    st.session_state["schema_locked"] = snapshot["schema_locked"]
    
    st.session_state["fast_issues"] = list(snapshot["fast_issues"])
    st.session_state["deep_issues"] = list(snapshot["deep_issues"])
    st.session_state["ignored_issues"] = set(snapshot["ignored_issues"])
    
    st.session_state["fast_count_start"] = snapshot["fast_count_start"]
    st.session_state["deep_count_start"] = snapshot["deep_count_start"]
    
    # TRUNCATE History
    st.session_state["history"] = st.session_state["history"][:index+1]