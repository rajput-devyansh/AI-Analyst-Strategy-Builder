import streamlit as st
import polars as pl
import datetime

def init_session_state():
    """Initialize all session variables."""
    # DATA OBJECTS
    if "uploaded_file" not in st.session_state: st.session_state["uploaded_file"] = None  # <--- NEW: Stores raw file IO
    if "df" not in st.session_state: st.session_state["df"] = None
    
    # APP STATE
    if "history" not in st.session_state: st.session_state["history"] = []
    if "schema_locked" not in st.session_state: st.session_state["schema_locked"] = False
    if "app_stage" not in st.session_state: st.session_state["app_stage"] = "UPLOAD" 
    
    # ISSUE TRACKING
    if "fast_issues" not in st.session_state: st.session_state["fast_issues"] = []
    
    # Advanced Scan Issue Lists
    if "vocab_issues" not in st.session_state: st.session_state["vocab_issues"] = []
    if "stat_issues" not in st.session_state: st.session_state["stat_issues"] = []
    
    if "deep_issues" not in st.session_state: st.session_state["deep_issues"] = []
    if "ignored_issues" not in st.session_state: st.session_state["ignored_issues"] = set()
    
    # KPI Baselines
    if "fast_count_start" not in st.session_state: st.session_state["fast_count_start"] = 0
    if "deep_count_start" not in st.session_state: st.session_state["deep_count_start"] = 0
    
    # Structure Lab State (Step 1.5)
    if "header_idx" not in st.session_state: st.session_state["header_idx"] = 0

def save_checkpoint(action_summary: str, details: str = None, category: str = "General", sub_category: str = None, changeset: str = None):
    """
    Saves a snapshot including DATA, SCAN RESULTS, and IGNORED REGISTRY.
    """
    if st.session_state["df"] is not None:
        
        # --- PREVENT DUPLICATES ---
        if len(st.session_state["history"]) > 0:
            last_entry = st.session_state["history"][-1]
            if last_entry["summary"] == action_summary and last_entry["details"] == details:
                return

        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Clone complex objects
        fast_issues_snapshot = list(st.session_state["fast_issues"])
        vocab_issues_snapshot = list(st.session_state["vocab_issues"])
        stat_issues_snapshot = list(st.session_state["stat_issues"])
        deep_issues_snapshot = list(st.session_state["deep_issues"])
        ignored_snapshot = set(st.session_state["ignored_issues"])
        
        snapshot = {
            "timestamp": ts,
            "df": st.session_state["df"].clone(),
            # We don't clone uploaded_file (it's immutable IO), just reference it
            "uploaded_file": st.session_state.get("uploaded_file"), 
            
            "summary": action_summary,
            "details": details,
            "category": category, 
            "sub_category": sub_category,
            "changeset": changeset,
            
            "stage_state": st.session_state["app_stage"],
            "schema_locked": st.session_state["schema_locked"],
            "header_idx": st.session_state.get("header_idx", 0), # Save Structure State
            
            "fast_issues": fast_issues_snapshot,
            "vocab_issues": vocab_issues_snapshot,
            "stat_issues": stat_issues_snapshot,
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
    
    # Restore File Pointer (Critical if undoing back to Structure Step)
    if snapshot.get("uploaded_file"):
        st.session_state["uploaded_file"] = snapshot["uploaded_file"]
        
    st.session_state["app_stage"] = snapshot["stage_state"]
    st.session_state["schema_locked"] = snapshot["schema_locked"]
    st.session_state["header_idx"] = snapshot.get("header_idx", 0)
    
    st.session_state["fast_issues"] = list(snapshot["fast_issues"])
    st.session_state["vocab_issues"] = list(snapshot["vocab_issues"])
    st.session_state["stat_issues"] = list(snapshot["stat_issues"])
    
    st.session_state["deep_issues"] = list(snapshot["deep_issues"])
    st.session_state["ignored_issues"] = set(snapshot["ignored_issues"])
    
    st.session_state["fast_count_start"] = snapshot["fast_count_start"]
    st.session_state["deep_count_start"] = snapshot["deep_count_start"]
    
    # TRUNCATE History
    st.session_state["history"] = st.session_state["history"][:index+1]