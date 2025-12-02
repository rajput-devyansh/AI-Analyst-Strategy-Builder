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

def get_current_schema_view(df: pl.DataFrame):
    """
    Returns a simple Pandas DataFrame summary of the current schema.
    """
    schema_data = []
    for col in df.columns:
        poly_type = str(df[col].dtype)
        friendly_type = REVERSE_MAPPING.get(poly_type, "Text (String)")
        schema_data.append({"Column Name": col, "Data Type": friendly_type})
    return pd.DataFrame(schema_data)

def get_column_info(df: pl.DataFrame, col_name: str):
    """
    Returns details for a single column to display in the card.
    """
    poly_type = str(df[col_name].dtype)
    friendly_type = REVERSE_MAPPING.get(poly_type, "Text (String)")
    
    # Get a sample non-null value
    sample_val = df[col_name].drop_nulls().head(1).to_list()
    raw_sample = str(sample_val[0]) if sample_val else "null"
    
    return friendly_type, raw_sample

def get_preview_value(val, target_type_str):
    """
    Tries to cast a single value to show what will happen.
    Now smarter about String->Int and String->Bool conversions.
    """
    if val == "null" or val is None:
        return "null"
    
    target_type = TYPE_MAPPING.get(target_type_str, pl.String)
    
    try:
        # --- SMART PREVIEW LOGIC ---
        
        # 1. Handle Float-String to Integer ("59.84" -> 59)
        if target_type == pl.Int64 and "." in val:
            try:
                # Try converting to float first, then int (mimics Polars float->int behavior)
                float_val = float(val)
                return str(int(float_val))
            except ValueError:
                pass # Not a number, let standard cast fail

        # 2. Handle "Yes"/"No" to Boolean
        if target_type == pl.Boolean:
            lower_val = val.lower()
            if lower_val in ["yes", "y", "true", "1"]:
                return "true"
            if lower_val in ["no", "n", "false", "0"]:
                return "false"

        # 3. Standard Polars Cast
        s = pl.Series([val])
        casted = s.cast(target_type, strict=False)
        result = casted[0]
        return str(result) if result is not None else "null (Cast Failed)"
        
    except:
        return "Error"

def cast_single_column(df: pl.DataFrame, col_name: str, target_type_str: str):
    """
    Updates the dataframe immediately for one column.
    """
    target_type = TYPE_MAPPING.get(target_type_str, pl.String)
    try:
        # Apply cast
        new_df = df.with_columns(pl.col(col_name).cast(target_type, strict=False))
        return new_df, None
    except Exception as e:
        return df, str(e)