import polars as pl
import json

def load_data(uploaded_file):
    try:
        if uploaded_file.name.endswith('.csv'):
            # ignore_errors=True is REQUIRED for your Supply Chain "Bad Row" test case
            return pl.read_csv(uploaded_file, ignore_errors=True, try_parse_dates=True, infer_schema_length=10000)
        else:
            return pl.read_excel(uploaded_file)
    except Exception as e:
        return f"Error loading file: {e}"

def get_data_profile(df: pl.DataFrame):
    """
    Stats for the AI to understand column ranges.
    """
    profile_data = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        stats = {"type": dtype, "nulls": df[col].null_count()}
        
        # Get Min/Max to help AI detect "Age: -5" or "Year: 2099"
        if dtype in ["Int64", "Float64", "Int32", "Float32"]:
            stats["min"] = df[col].min()
            stats["max"] = df[col].max()
            
        profile_data[col] = stats
    return json.dumps(profile_data, indent=2, default=str)