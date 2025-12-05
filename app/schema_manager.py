import polars as pl
import pandas as pd
from app.utils import to_snake_case

# ==========================================
# 1. LEGACY UTILS (For Auto-Detect Mode)
# ==========================================

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
    """Returns a simple Pandas DataFrame summary of the current schema."""
    schema_data = []
    for col in df.columns:
        poly_type = str(df[col].dtype)
        friendly_type = REVERSE_MAPPING.get(poly_type, "Text (String)")
        schema_data.append({"Column Name": col, "Data Type": friendly_type})
    return pd.DataFrame(schema_data)

def get_column_info(df: pl.DataFrame, col_name: str):
    """Returns details for a single column to display in the card."""
    poly_type = str(df[col_name].dtype)
    friendly_type = REVERSE_MAPPING.get(poly_type, "Text (String)")
    
    # Get a sample non-null value
    sample_val = df[col_name].drop_nulls().head(1).to_list()
    raw_sample = str(sample_val[0]) if sample_val else "null"
    return friendly_type, raw_sample

def get_preview_value(val, target_type_str):
    """Tries to cast a single value to show what will happen."""
    if val == "null" or val is None: return "null"
    target_type = TYPE_MAPPING.get(target_type_str, pl.String)
    try:
        if target_type == pl.Int64 and "." in val:
            try: return str(int(float(val)))
            except: pass
        if target_type == pl.Boolean:
            if val.lower() in ["yes", "y", "true", "1"]: return "true"
            if val.lower() in ["no", "n", "false", "0"]: return "false"
        s = pl.Series([val])
        casted = s.cast(target_type, strict=False)
        result = casted[0]
        return str(result) if result is not None else "null (Cast Failed)"
    except: return "Error"

def cast_single_column(df: pl.DataFrame, col_name: str, target_type_str: str):
    """Updates the dataframe immediately for one column."""
    target_type = TYPE_MAPPING.get(target_type_str, pl.String)
    try:
        new_df = df.with_columns(pl.col(col_name).cast(target_type, strict=False))
        return new_df, None
    except Exception as e:
        return df, str(e)

# ==========================================
# 2. VALIDATION ENGINE (For Manual Mode)
# ==========================================

DOMAIN_TYPES = {
    "Select Intent...": None,
    "Alphabetic (A-Z)": "alpha",
    "Alphanumeric (A-Z, 0-9)": "alnum",
    "Alphanumeric + Symbols": "alnum_sym", # NEW
    "Numeric (Integer)": "int",
    "Numeric (Financial/Float)": "float",
    "Email Address": "email",
    "Phone Number": "phone",
    "Date (Standardized)": "date",
    "Text (General)": "text",
    "Boolean": "bool"
}

def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """Renames all columns to snake_case."""
    new_cols = {col: to_snake_case(col) for col in df.columns}
    return df.rename(new_cols)

def check_domain_constraints(df: pl.DataFrame, col: str, domain_type: str, allowed_domains: list = None):
    """
    Checks if a column satisfies the chosen Domain Type.
    Supports optional allowed_domains list for Emails.
    """
    s = df[col].cast(pl.String) 
    total = df.height
    invalid_mask = None
    suggestion = ""

    # --- 1. ALPHABETIC ---
    if domain_type == "alpha":
        invalid_mask = ~s.str.contains(r"^[a-zA-Z\s]*$")
        suggestion = "Remove numbers/symbols or switch to Alphanumeric."

    # --- 2. ALPHANUMERIC ---
    elif domain_type == "alnum":
        invalid_mask = ~s.str.contains(r"^[a-zA-Z0-9\s]*$")
        suggestion = "Remove special characters."

    # --- 3. ALPHANUMERIC + SYMBOLS (New) ---
    elif domain_type == "alnum_sym":
        # Allows A-Z, 0-9, space, and common symbols: - _ . , # & ( ) /
        # Excludes @ to distinguish from Email intent if desired, though strictly regex permits what we define
        invalid_mask = ~s.str.contains(r"^[a-zA-Z0-9\s\-\_\.\,\#\&\(\)\/]*$")
        suggestion = "Remove unsupported special characters."

    # --- 4. NUMERIC (FINANCIAL) ---
    elif domain_type == "float" or domain_type == "int":
        cleaned = s.str.replace_all(r"[$,]", "")
        is_float = cleaned.cast(pl.Float64, strict=False).is_not_null()
        was_not_null = s.is_not_null()
        invalid_mask = was_not_null & ~is_float
        suggestion = "Strip Currency Symbols ($) and Commas (,)."

    # --- 5. EMAIL ---
    elif domain_type == "email":
        # Base Check: Contains @ and .
        base_mask = s.str.contains(r"^.+@.+\..+$")
        
        if allowed_domains and len(allowed_domains) > 0:
            # Clean domains list
            domains_pattern = "|".join([d.strip().replace(".", r"\.") for d in allowed_domains if d.strip()])
            # Check if ends with one of the domains
            domain_mask = s.str.contains(rf"@(?:{domains_pattern})$")
            invalid_mask = ~(base_mask & domain_mask)
            suggestion = f"Must contain @ and match domains: {', '.join(allowed_domains)}"
        else:
            invalid_mask = ~base_mask
            suggestion = "Drop rows or fix typos (missing @)."

    # --- 6. PHONE ---
    elif domain_type == "phone":
        digits_only = s.str.replace_all(r"[^0-9]", "")
        invalid_mask = digits_only.str.len_bytes() < 7
        suggestion = "Extract digits and format as (XXX) XXX-XXXX."

    else:
        # Default or Text
        return total, 0, [], "None"

    # Calc Results
    invalid_rows = df.filter(invalid_mask)
    invalid_count = invalid_rows.height
    valid_count = total - invalid_count
    examples = invalid_rows[col].head(5).to_list()
    
    return valid_count, invalid_count, examples, suggestion

def apply_sanitization(df: pl.DataFrame, col: str, domain_type: str) -> pl.DataFrame:
    """
    Applies the Standard Fixes.
    Fixes Date crash by ensuring cast to String first.
    """
    if domain_type in ["float", "int"]:
        df = df.with_columns(
            pl.col(col).cast(pl.String).str.replace_all(r"[$,]", "").cast(pl.Float64, strict=False).alias(col)
        )
        if domain_type == "int":
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

    elif domain_type == "phone":
        df = df.with_columns(
            pl.col(col).cast(pl.String).str.replace_all(r"[^0-9]", "").alias(col)
        )
        
    elif domain_type == "email":
        df = df.with_columns(
            pl.col(col).cast(pl.String).str.strip_chars().str.to_lowercase().alias(col)
        )
        
    elif domain_type == "date":
        # CRASH FIX: Check if already Date, if so, do nothing or just return
        # If not date, cast to String FIRST, then to Date.
        dtype = df[col].dtype
        if dtype != pl.Date and dtype != pl.Datetime:
            df = df.with_columns(
                pl.col(col).cast(pl.String).str.to_date(strict=False).alias(col)
            )
        
    return df