import polars as pl
import pandas as pd

# Mapping friendly names to Polars types
TYPE_MAPPING = {
    "Text (String)": pl.String,
    "Integer (Int64)": pl.Int64,
    "Decimal (Float64)": pl.Float64,
    "Boolean (True/False)": pl.Boolean,
    "Date (YYYY-MM-DD)": pl.Date,
    "Datetime (Timestamp)": pl.Datetime
}

REVERSE_MAPPING = {
    "String": "Text (String)",
    "Int64": "Integer (Int64)",
    "Float64": "Decimal (Float64)",
    "Boolean": "Boolean (True/False)",
    "Date": "Date (YYYY-MM-DD)",
    "Datetime": "Datetime (Timestamp)"
}

def get_preview_value(val, target_type_str):
    """
    Tries to cast a single value to the target type to generate a preview.
    """
    if val is None:
        return "null"
        
    try:
        # Create a tiny 1-value Series to test the cast
        target_type = TYPE_MAPPING.get(target_type_str, pl.String)
        s = pl.Series([val])
        # Perform loose casting (strict=False) to mimic the actual operation
        casted = s.cast(target_type, strict=False)
        result = casted[0]
        return str(result) if result is not None else "null (Cast Failed)"
    except:
        return "Error"

def get_schema_summary(df: pl.DataFrame, current_edits=None):
    """
    Generates the UI table.
    - 'current_edits' is a DICTIONARY from st.session_state["schema_editor"].
    - Structure: {"edited_rows": {0: {"Target Type": "Integer"}}, ...}
    """
    summary_data = []
    
    # 1. Parse the Streamlit Editor Dictionary
    # We map the ROW INDEX (int) to the NEW VALUE
    edit_map = {}
    if current_edits and "edited_rows" in current_edits:
        for index, changes in current_edits["edited_rows"].items():
            if "Target Type" in changes:
                # Store: Row Index -> New Type String
                edit_map[int(index)] = changes["Target Type"]
    
    # 2. Build the Summary Table
    # We enumerate so we know the row index (0, 1, 2...) matches the edit_map
    for i, col in enumerate(df.columns):
        # A. Get Details
        poly_type = str(df[col].dtype)
        friendly_type = REVERSE_MAPPING.get(poly_type, "Text (String)")
        
        # B. Get Raw Sample (First non-null)
        sample_list = df[col].drop_nulls().head(1).to_list()
        raw_sample = sample_list[0] if sample_list else None
        raw_display = str(raw_sample) if raw_sample is not None else "null"
        
        # C. Determine Target Type 
        # Check if user edited this specific row index (i)
        target_type = edit_map.get(i, friendly_type)
        
        # D. Generate Preview
        preview_display = get_preview_value(raw_sample, target_type)
        
        summary_data.append({
            "Column Name": col,
            "Detected Type": friendly_type,
            "Actual Raw Format": raw_display,  
            "Target Type": target_type,        
            "Updated Format": preview_display 
        })
        
    return pd.DataFrame(summary_data)

def apply_schema_changes(df: pl.DataFrame, edited_schema_df: pd.DataFrame):
    """
    Applies the final types to the Polars DataFrame.
    Here 'edited_schema_df' IS a DataFrame (returned by st.data_editor value), so iterrows works.
    """
    casting_expressions = []
    error_logs = []
    
    for _, row in edited_schema_df.iterrows():
        col_name = row["Column Name"]
        target_str = row["Target Type"]
        
        if target_str in TYPE_MAPPING:
            target_type = TYPE_MAPPING[target_str]
            try:
                casting_expressions.append(pl.col(col_name).cast(target_type, strict=False))
            except Exception as e:
                error_logs.append(f"Failed to cast {col_name}: {e}")
            
    try:
        new_df = df.with_columns(casting_expressions)
        return new_df, error_logs
    except Exception as e:
        return df, [str(e)]