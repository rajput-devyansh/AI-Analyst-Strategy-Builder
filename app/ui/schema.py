import streamlit as st
from app.schema_manager import get_column_info, get_preview_value, cast_single_column, get_current_schema_view, TYPE_MAPPING
from app.state_manager import save_checkpoint

def render_schema_page():
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
            save_checkpoint("Schema Confirmed", "Locked data types.", "Audit", "System")
            st.session_state["schema_locked"] = True
            st.session_state["app_stage"] = "AUDIT"
            st.rerun()
    else:
        # Fallback if somehow locked but still on this page (shouldn't happen with Router)
        st.success("Schema is locked. Proceeding to Audit...")
        st.session_state["app_stage"] = "AUDIT"
        st.rerun()