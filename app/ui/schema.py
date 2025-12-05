import streamlit as st
import polars as pl
from app.schema_manager import (
    normalize_column_names, check_domain_constraints, apply_sanitization, DOMAIN_TYPES,
    get_column_info, get_preview_value, cast_single_column, get_current_schema_view, TYPE_MAPPING
)
from app.state_manager import save_checkpoint

def render_schema_page():
    st.title("🛠️ Step 2: Schema & Format Validation")
    
    df = st.session_state["df"]

    # --- STEP 2A: COLUMN NORMALIZATION (Snake Case) ---
    if not st.session_state.get("cols_normalized", False):
        st.info("First, let's standardize your column names (Snake Case) for better analysis.")
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Current Names")
            st.write(df.columns)
        with c2:
            st.subheader("Preview New Names")
            temp_df = normalize_column_names(df.clone())
            st.write(temp_df.columns)
            
        if st.button("Normalize & Proceed"):
            st.session_state["df"] = normalize_column_names(df)
            st.session_state["cols_normalized"] = True
            save_checkpoint("Columns Normalized", "Converted to snake_case", "Schema", "System")
            st.rerun()
        return

    # --- STEP 2B: THE FORK (Auto vs Manual) ---
    if "schema_mode" not in st.session_state:
        st.subheader("How would you like to validate your data?")
        
        c1, c2 = st.columns(2)
        with c1:
            with st.container(border=True):
                st.markdown("### 🤖 Auto-Detect (Fast)")
                st.write("Best for clean files. We will guess data types automatically.")
                if st.button("Use Auto-Detect"):
                    st.session_state["schema_mode"] = "AUTO"
                    # NEW: Log the choice to System History
                    save_checkpoint("Schema Mode: Auto", "User selected Auto-Detect", "Schema", "System")
                    st.rerun()
                    
        with c2:
            with st.container(border=True):
                st.markdown("### 🛠️ Manual Contract (Deep)")
                st.write("Best for messy files. You define what each column *should* be.")
                if st.button("Start Manual Validation"):
                    st.session_state["schema_mode"] = "MANUAL"
                    st.session_state["current_col_idx"] = 0 
                    st.session_state["validation_result"] = None
                    # NEW: Log the choice to System History
                    save_checkpoint("Schema Mode: Manual", "User selected Manual Validation", "Schema", "System")
                    st.rerun()
        return

    # ==========================================
    # MODE: AUTO (RESTORED LEGACY VIEW)
    # ==========================================
    if st.session_state["schema_mode"] == "AUTO":
        
        with st.expander("📄 View Initial Detected Schema", expanded=True):
            if "initial_schema_view" in st.session_state:
                st.dataframe(st.session_state["initial_schema_view"], use_container_width=True, hide_index=True)
            else:
                st.caption("No initial snapshot available.")

        st.subheader("Modify Columns")
        st.info("Review and correct any types below.")

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
        st.subheader("Final Schema Snapshot")
        current_view = get_current_schema_view(df)
        st.dataframe(current_view, use_container_width=True, hide_index=True)

        if st.button("✅ Confirm Schema & Proceed", type="primary"):
            save_checkpoint("Schema Confirmed", "Locked data types.", "Audit", "System")
            st.session_state["schema_locked"] = True
            st.session_state["app_stage"] = "AUDIT"
            st.rerun()
            
    # ==========================================
    # MODE: MANUAL WIZARD (INTERACTIVE)
    # ==========================================
    elif st.session_state["schema_mode"] == "MANUAL":
        
        cols = df.columns
        curr_idx = st.session_state.get("current_col_idx", 0)
        
        # --- COMPLETION STATE ---
        if curr_idx >= len(cols):
            st.success("🎉 All columns validated!")
            
            st.markdown("---")
            st.subheader("Final Schema Snapshot")
            current_view = get_current_schema_view(df)
            st.dataframe(current_view, use_container_width=True, hide_index=True)
            
            if st.button("Finish Schema Step", type="primary"):
                st.session_state["schema_locked"] = True
                st.session_state["app_stage"] = "AUDIT"
                st.rerun()
            return

        current_col = cols[curr_idx]
        
        # --- WIZARD UI ---
        st.progress((curr_idx) / len(cols), text=f"Validating Column {curr_idx + 1} of {len(cols)}: '{current_col}'")
        
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.subheader(f"Column: `{current_col}`")
            st.caption("Live Data Preview (Top 20)")
            # Reverted to raw dataframe preview
            st.dataframe(df.select(current_col).head(20), use_container_width=True)

        with col_right:
            st.markdown("### 1. Define Intent")
            
            # Intent Selection
            selected_domain_key = st.selectbox(
                "What kind of data is this?",
                list(DOMAIN_TYPES.keys()), # "Select Intent..." is now first
                index=0,
                key=f"domain_{curr_idx}"
            )
            
            domain_code = DOMAIN_TYPES[selected_domain_key]
            
            # --- SPECIAL HANDLING: EMAIL DOMAINS ---
            allowed_domains = []
            if domain_code == "email":
                with st.expander("📧 Advanced Verification (Optional)", expanded=False):
                    st.caption("If you know the specific domains allowed (e.g. your company), list them here.")
                    domain_input = st.text_input("Allowed Domains (comma separated)", placeholder="gmail.com, company.com")
                    if domain_input:
                        allowed_domains = [d.strip() for d in domain_input.split(",") if d.strip()]

            # --- VALIDATION ACTION ---
            # Disable button if user hasn't selected a valid intent (domain_code is None)
            btn_disabled = domain_code is None
            
            if st.button("🔍 Run Validation Test", type="primary", use_container_width=True, disabled=btn_disabled):
                 valid_count, invalid_count, examples, suggestion = check_domain_constraints(
                     df, current_col, domain_code, allowed_domains=allowed_domains
                 )
                 
                 st.session_state["validation_result"] = {
                     "valid": valid_count,
                     "invalid": invalid_count,
                     "examples": examples,
                     "suggestion": suggestion,
                     "col": current_col,
                     "domain_code": domain_code # Store this to re-apply if needed
                 }

            # SHOW RESULTS (Only if validation was run for THIS column)
            res = st.session_state.get("validation_result")
            
            if res and res["col"] == current_col:
                st.divider()
                st.markdown("### 2. Verification Results")
                
                if res["invalid"] == 0:
                    st.success(f"✅ 100% Match ({res['valid']} rows)")
                    
                    st.markdown("### 3. Confirm Data Type")
                    # Smart Default based on Domain Intent
                    type_options = list(TYPE_MAPPING.keys())
                    default_type = "Text (String)"
                    
                    if res["domain_code"] in ["int"]: default_type = "Integer (Int64)"
                    elif res["domain_code"] in ["float"]: default_type = "Decimal (Float64)"
                    elif res["domain_code"] in ["bool"]: default_type = "Boolean (True/False)"
                    elif res["domain_code"] in ["date"]: default_type = "Date (YYYY-MM-DD)"
                    
                    # Try to find index of default, else 0
                    def_idx = type_options.index(default_type) if default_type in type_options else 0
                    
                    selected_type = st.selectbox("Cast Column To:", type_options, index=def_idx, key=f"final_type_{curr_idx}")
                    
                    if st.button("Confirm Type & Next ➡️", type="primary"):
                        # 1. Apply Sanitization (Standard formatting)
                        temp_df = apply_sanitization(df, current_col, res["domain_code"])
                        
                        # 2. Apply Strict Casting to User Selection
                        final_df, err = cast_single_column(temp_df, current_col, selected_type)
                        
                        if err:
                            st.error(f"Casting Error: {err}")
                        else:
                            st.session_state["df"] = final_df
                            
                            # NEW: Log the Type Confirmation to History
                            # "Schema Updates" sub_category ensures it appears in the sidebar under "Type Changes"
                            save_checkpoint(
                                f"Confirmed '{current_col}'", 
                                f"Type set to {selected_type}", 
                                "Schema", 
                                "Schema Updates"
                            )

                            st.session_state["current_col_idx"] += 1
                            st.session_state["validation_result"] = None
                            st.rerun()
                            
                else:
                    st.error(f"❌ {res['invalid']} Invalid Rows detected!")
                    st.warning(f"Examples: {res['examples']}")
                    # Only show specific suggestion if it's relevant (mostly for Int/Float)
                    if res["domain_code"] in ["int", "float"] and res['suggestion'] != "None":
                         st.info(f"💡 Suggestion: {res['suggestion']}")
                    
                    # --- SMART SUGGESTION LOGIC (IMPROVED) ---
                    # Quick check if it fits another strict type better
                    sample_df = df.select(current_col).drop_nulls().head(1000)
                    if sample_df.height > 0:
                        strict_domains = [
                            ("int", "Numeric (Integer)"),
                            ("float", "Numeric (Financial)"),
                            ("date", "Date"),
                            ("alpha", "Alphabetic"),
                            ("alnum", "Alphanumeric")
                        ]
                        
                        found_better = False
                        for code, label in strict_domains:
                            if code == res["domain_code"]: continue # Skip current
                            v, inv, _, _ = check_domain_constraints(sample_df, current_col, code)
                            if inv == 0:
                                st.info(f"🤔 This looks like **{label}**. Consider changing the Intent above.")
                                found_better = True
                                break
                        
                        # Only suggest Text if NO other strict type matches
                        if not found_better:
                            st.info("💡 **Suggestion:** This column contains custom or mixed data. Please change Intent to **Text (General)**.")

                    # --- RESTORED ACTION TAB ---
                    st.markdown("### 3. Action")
                    c_fix, c_skip = st.columns(2)
                    with c_fix:
                        if st.button("🪄 Auto-Fix"):
                            new_df = apply_sanitization(df, current_col, res["domain_code"])
                            st.session_state["df"] = new_df
                            
                            save_checkpoint(f"Sanitized {current_col}", f"Applied {res['domain_code']} rules", "Schema", "Fix")
                            
                            # Re-run validation logic to verify fix
                            valid, invalid, ex, sugg = check_domain_constraints(new_df, current_col, res["domain_code"], allowed_domains)
                            
                            st.session_state["validation_result"] = {
                                "valid": valid,
                                "invalid": invalid,
                                "examples": ex,
                                "suggestion": sugg,
                                "col": current_col,
                                "domain_code": res["domain_code"]
                            }
                            st.rerun() 
                            
                    with c_skip:
                        if st.button("Skip / Keep As Is"):
                            st.session_state["current_col_idx"] += 1
                            st.session_state["validation_result"] = None
                            st.rerun()
                        
        # Back Button
        if curr_idx > 0:
            st.divider()
            if st.button("⬅️ Back"):
                st.session_state["current_col_idx"] -= 1
                st.session_state["validation_result"] = None
                st.rerun()