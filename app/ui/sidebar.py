import streamlit as st
from app.state_manager import restore_checkpoint

def render_history_item(i, event):
    # Determine icon and text
    is_latest = (i == len(st.session_state["history"]) - 1)
    btn_label = "↩️" if is_latest else "⏪"
    help_text = "Undo this change" if is_latest else "⚠️ Reverting here undoes subsequent actions."
    
    c1, c2 = st.columns([5, 1])
    with c1:
        st.markdown(f"**{event['summary']}**")
        if event['details']: st.caption(f"{event['details']}")
        
        # --- Show Impact (Rows Dropped/Changed) ---
        if event.get('changeset'): 
            st.caption(f"📉 {event['changeset']}")
            
        st.caption(f"_{event['timestamp']}_")
    with c2:
        if st.button(btn_label, key=f"hist_{i}", help=help_text):
            restore_checkpoint(i-1 if i > 0 else 0)
            st.rerun()
    st.divider()

def render_sidebar():
    with st.sidebar:
        st.title("📜 Audit Trail")
        
        history = st.session_state["history"]
        if not history:
            st.info("No actions recorded yet.")
            return

        # --- DETERMINE LAST ACTION FOR AUTO-EXPAND ---
        last_event = history[-1] if history else None
        last_cat = last_event['category'] if last_event else None
        last_sub = last_event['sub_category'] if last_event else None

        # =========================================================
        # STEP 3: AUDIT
        # =========================================================
        audit_events = [(i, e) for i, e in enumerate(history) if e["category"] == "Audit"]
        if audit_events:
            # Main Step 3 expands if we are currently in Audit mode
            is_active = st.session_state["app_stage"] == "AUDIT"
            with st.expander("Step 3: AI Audit", expanded=is_active):
                
                structural = [x for x in audit_events if x[1].get('sub_category') == 'Structural']
                logic = [x for x in audit_events if x[1].get('sub_category') == 'Logic']
                scans = [x for x in audit_events if x[1].get('sub_category') == 'Scan']
                system = [x for x in audit_events if x[1].get('sub_category') == 'System']

                # Dynamic Expand Logic
                exp_struct = (last_cat == "Audit" and last_sub == "Structural")
                exp_logic = (last_cat == "Audit" and last_sub == "Logic")
                exp_scan = (last_cat == "Audit" and last_sub == "Scan")

                if structural:
                    with st.expander(f"⚡ Structural Fixes ({len(structural)})", expanded=exp_struct):
                        for i, event in reversed(structural): render_history_item(i, event)
                if logic:
                    with st.expander(f"🧠 Logic Fixes ({len(logic)})", expanded=exp_logic):
                        for i, event in reversed(logic): render_history_item(i, event)
                if scans:
                    with st.expander(f"🔍 Scan Runs ({len(scans)})", expanded=exp_scan):
                        for i, event in reversed(scans): render_history_item(i, event)
                if system:
                    with st.expander(f"ℹ️ System Logs ({len(system)})", expanded=False):
                        for i, event in reversed(system): render_history_item(i, event)

        # =========================================================
        # STEP 2: SCHEMA (UPDATED)
        # =========================================================
        schema_events = [(i, e) for i, e in enumerate(history) if e["category"] == "Schema"]
        if schema_events:
            is_active = st.session_state["app_stage"] == "SCHEMA"
            with st.expander("Step 2: Schema Validation", expanded=is_active):
                
                # Auto Mode Updates
                updates = [x for x in schema_events if x[1].get('sub_category') == 'Schema Updates']
                # Manual Mode Fixes (NEW)
                fixes = [x for x in schema_events if x[1].get('sub_category') == 'Fix']
                # System (Normalization, etc.)
                system = [x for x in schema_events if x[1].get('sub_category') == 'System']
                
                # Expand Logic
                exp_updates = (last_cat == "Schema" and last_sub == "Schema Updates")
                exp_fixes = (last_cat == "Schema" and last_sub == "Fix")
                exp_sys = (last_cat == "Schema" and last_sub == "System")
                
                if updates:
                    with st.expander(f"✏️ Type Changes ({len(updates)})", expanded=exp_updates):
                        for i, event in reversed(updates): render_history_item(i, event)
                
                # NEW SECTION FOR MANUAL FIXES
                if fixes:
                    with st.expander(f"🪄 Format Fixes ({len(fixes)})", expanded=exp_fixes):
                        for i, event in reversed(fixes): render_history_item(i, event)

                if system:
                    with st.expander(f"ℹ️ System Logs ({len(system)})", expanded=exp_sys):
                        for i, event in reversed(system): render_history_item(i, event)

        # =========================================================
        # STEP 1: INGESTION
        # =========================================================
        ingestion_events = [(i, e) for i, e in enumerate(history) if e["category"] == "Ingestion"]
        if ingestion_events:
            is_active = st.session_state["app_stage"] == "UPLOAD"
            with st.expander("Step 1: Data Ingestion", expanded=is_active):
                for i, event in reversed(ingestion_events): render_history_item(i, event)

        st.markdown("---")
        
        # DOWNLOAD BUTTON
        if st.session_state["df"] is not None:
            csv = st.session_state["df"].write_csv()
            st.download_button(
                "💾 Download Clean Data",
                data=csv,
                file_name="clean_data.csv",
                mime="text/csv",
                use_container_width=True
            )

        if history and st.button("♻️ Reset Project", use_container_width=True):
            st.session_state.clear()
            st.rerun()