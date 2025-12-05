import polars as pl
import json
import re

def to_snake_case(text: str) -> str:
    """
    Converts 'Client Name' -> 'client_name'.
    Handles special characters and spaces.
    """
    # 1. Lowercase
    text = text.lower()
    # 2. Replace non-alphanumeric with underscore
    text = re.sub(r'[^a-z0-9]', '_', text)
    # 3. Collapse multiple underscores
    text = re.sub(r'_+', '_', text)
    # 4. Strip leading/trailing underscores
    return text.strip('_')

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

def validate_input(val, dtype_str):
    """
    Validates if the input value matches the column's data type.
    """
    if not val: return True, val 
    
    # 1. Numeric Check
    is_numeric_col = "Int" in dtype_str or "Float" in dtype_str
    if is_numeric_col:
        try:
            float(val)
            return True, val
        except ValueError:
            return False, None
            
    return True, val