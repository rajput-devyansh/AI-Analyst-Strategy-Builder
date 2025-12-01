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

def get_schema_summary(df: pl.DataFrame):
    """
    Creates a Pandas DataFrame describing the Polars DataFrame.
    This is what we will show in the UI editor.
    """
    summary_data = []
    
    for col in df.columns:
        # Get current type as string
        poly_type = str(df[col].dtype)
        
        # Map to friendly name (Default to Text if unknown)
        friendly_type = REVERSE_MAPPING.get(poly_type, "Text (String)")
        
        # Get a sample non-null value
        sample_val = df[col].drop_nulls().head(1).to_list()
        sample = str(sample_val[0]) if sample_val else "All Null"
        
        summary_data.append({
            "Column Name": col,
            "Detected Type": friendly_type,
            "Target Type": friendly_type, # Default to detected
            "Sample Data": sample
        })
        
    return pd.DataFrame(summary_data)

def apply_schema_changes(df: pl.DataFrame, edited_schema_df: pd.DataFrame):
    """
    Takes the user's edits and applies them to the Polars DataFrame.
    """
    casting_expressions = []
    error_logs = []
    
    for _, row in edited_schema_df.iterrows():
        col_name = row["Column Name"]
        target_str = row["Target Type"]
        target_type = TYPE_MAPPING[target_str]
        
        # Only cast if different or strict enforcement is needed
        try:
            # We use strict=False so bad data becomes null instead of crashing
            # This is safer for "Dirty Data" projects
            casting_expressions.append(pl.col(col_name).cast(target_type, strict=False))
        except Exception as e:
            error_logs.append(f"Failed to prep cast for {col_name}: {e}")
            
    # Apply all casts at once (lazy execution style)
    try:
        new_df = df.with_columns(casting_expressions)
        return new_df, error_logs
    except Exception as e:
        return df, [str(e)]