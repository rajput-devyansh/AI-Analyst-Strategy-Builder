import polars as pl
import io

def load_data(uploaded_file):
    """
    Loads raw data into Polars (High Performance).
    """
    try:
        if uploaded_file.name.endswith('.csv'):
            # try_parse_dates=True attempts to fix date formats automatically
            return pl.read_csv(uploaded_file, ignore_errors=True, try_parse_dates=True)
        else:
            return pl.read_excel(uploaded_file)
    except Exception as e:
        return f"Error loading file: {e}"

def get_data_profile(df: pl.DataFrame):
    """
    Generates a concise metadata summary for the AI.
    """
    # 1. Capture Schema & Types
    schema_info = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
    
    # 2. Count Nulls
    null_counts = {col: df[col].null_count() for col in df.columns if df[col].null_count() > 0}
    
    # 3. Get Sample Rows (convert to markdown for readability)
    sample_rows = df.head(3).to_pandas().to_markdown(index=False)
    
    profile = f"""
    DATASET SHAPE: {df.height} rows, {df.width} columns.
    
    COLUMNS & TYPES:
    {schema_info}
    
    MISSING VALUES (Nulls):
    {null_counts}
    
    SAMPLE DATA:
    {sample_rows}
    """
    return profile