import streamlit as st
import polars as pl

def init_session_state():
    """Initialize all session variables."""
    if "df" not in st.session_state: st.session_state["df"] = None
    if "history" not in st.session_state: st.session_state["history"] = []
    if "schema_locked" not in st.session_state: st.session_state["schema_locked"] = False
    if "app_stage" not in st.session_state: st.session_state["app_stage"] = "UPLOAD" # UPLOAD, SCHEMA, AUDIT
    if "fast_issues" not in st.session_state: st.session_state["fast_issues"] = []
    if "deep_issues" not in st.session_state: st.session_state["deep_issues"] = []

def save_checkpoint(action_name: str):
    """
    Saves the current state BEFORE a change is applied.
    This allows us to go back to this state.
    """
    if st.session_state["df"] is not None:
        # We clone the dataframe to ensure it's a separate copy in memory
        snapshot = {
            "df": st.session_state["df"].clone(),
            "desc": action_name,
            "stage": st.session_state["app_stage"],
            "schema_locked": st.session_state["schema_locked"]
        }
        st.session_state["history"].append(snapshot)

def restore_checkpoint(index: int):
    """
    Reverts the app to the state saved at 'index'.
    Drops all history that happened AFTER this point.
    """
    # Get the snapshot we want to go back to
    snapshot = st.session_state["history"][index]
    
    # Restore variables
    st.session_state["df"] = snapshot["df"]
    st.session_state["app_stage"] = snapshot["stage"]
    st.session_state["schema_locked"] = snapshot["schema_locked"]
    
    # Clear derived data (scans need to re-run on old data)
    st.session_state["fast_issues"] = []
    st.session_state["deep_issues"] = []
    
    # Cut the history list to keep only up to this point
    # We remove the snapshot itself from history because we are currently LIVING in it
    # (The next action will save it again as a previous state)
    st.session_state["history"] = st.session_state["history"][:index]