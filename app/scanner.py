import polars as pl
import polars.selectors as cs

def get_fix_strategies(issue_type, dtype):
    """
    Returns fix options strictly based on the Polars DataType groups.
    """
    options = []
    
    # Check if type is numeric (Int8, Int64, Float32, etc.)
    is_numeric = dtype.is_numeric()
    is_string = dtype == pl.String
    is_bool = dtype == pl.Boolean
    is_date = dtype in [pl.Date, pl.Datetime]

    # 1. MISSING VALUES (NULLS)
    if issue_type == "Missing Values":
        
        # --- NUMERIC STRATEGIES ---
        if is_numeric:
            options = [
                "Fill with Median", 
                "Fill with Mean", 
                "Fill with Mode", # Valid for numbers too (most frequent number)
                "Fill with 0", 
                "Fill with -1", 
                "Forward Fill",
                "Drop Rows"
            ]
            
        # --- STRING STRATEGIES ---
        elif is_string:
            options = [
                "Fill with 'Unknown'", 
                "Fill with 'Missing'", 
                "Fill with Mode", # Mode is valid for text (e.g. most common City)
                "Forward Fill", 
                "Drop Rows"
            ]
            
        # --- BOOLEAN STRATEGIES ---
        elif is_bool:
            options = [
                "Fill with False",
                "Fill with True",
                "Fill with Mode",
                "Forward Fill",
                "Drop Rows"
            ]
            
        # --- DATE STRATEGIES ---
        elif is_date:
            options = [
                "Forward Fill", 
                "Backward Fill", 
                "Drop Rows"
            ]
            
        # --- FALLBACK ---
        else:
            options = ["Drop Rows"]

    # 2. DUPLICATES (Type Independent)
    elif issue_type == "Duplicate Rows":
        options = ["Remove Duplicates", "Keep First", "Keep Last"]

    # 3. NEGATIVE VALUES (Numeric Only)
    elif issue_type == "Negative Values":
        # We double check strictly here
        if is_numeric:
            options = [
                "Convert to Absolute", 
                "Replace with 0", 
                "Replace with Mean", 
                "Drop Rows"
            ]

    return options

def scan_structural_issues(df: pl.DataFrame):
    """
    Scans for issues and attaches strict options.
    """
    issues = []
    
    # 1. Null Check
    for col in df.columns:
        null_count = df[col].null_count()
        if null_count > 0:
            # Pass the actual Polars dtype object
            strats = get_fix_strategies("Missing Values", df[col].dtype)
            issues.append({
                "type": "Missing Values",
                "column": col,
                "count": null_count,
                "dtype": str(df[col].dtype),
                "options": strats
            })

    # 2. Duplicate Check
    dup_count = df.is_duplicated().sum()
    if dup_count > 0:
        strats = get_fix_strategies("Duplicate Rows", pl.Object) # Type doesn't matter
        issues.append({
            "type": "Duplicate Rows",
            "column": "Dataset",
            "count": dup_count,
            "dtype": "N/A",
            "options": strats
        })

    # 3. Numeric Negatives
    try:
        # Strict selector for numeric columns only
        numeric_cols = df.select(cs.numeric()).columns
        for col in numeric_cols:
            neg_count = df.filter(pl.col(col) < 0).height
            if neg_count > 0:
                strats = get_fix_strategies("Negative Values", df[col].dtype)
                issues.append({
                    "type": "Negative Values",
                    "column": col,
                    "count": neg_count,
                    "dtype": str(df[col].dtype),
                    "options": strats
                })
    except:
        pass
            
    return issues