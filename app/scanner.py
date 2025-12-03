import polars as pl
import polars.selectors as cs

def get_fix_strategies(issue_type, dtype):
    """
    Returns fix options strictly based on the Polars DataType groups.
    """
    options = []
    
    # Check types strictly
    is_numeric = dtype.is_numeric()
    is_string = dtype == pl.String
    is_bool = dtype == pl.Boolean
    is_date = dtype in [pl.Date, pl.Datetime]

    # 1. MISSING VALUES (NULLS)
    if issue_type == "Missing Values":
        
        if is_numeric:
            options = ["Fill with Median", "Fill with Mean", "Fill with Mode", "Fill with 0", "Fill with -1", "Forward Fill", "Drop Rows (Nulls)"]
            
        elif is_string:
            options = ["Fill with 'Unknown'", "Fill with 'Missing'", "Fill with Mode", "Forward Fill", "Drop Rows (Nulls)"]
            
        elif is_bool:
            options = ["Fill with False", "Fill with True", "Fill with Mode", "Forward Fill", "Drop Rows (Nulls)"]
            
        elif is_date:
            options = ["Forward Fill", "Backward Fill", "Drop Rows (Nulls)"]
            
        else:
            options = ["Drop Rows (Nulls)"]

    # 2. DUPLICATES
    elif issue_type == "Duplicate Rows":
        options = ["Remove Duplicates", "Keep First", "Keep Last"]

    # 3. NEGATIVE VALUES
    elif issue_type == "Negative Values":
        if is_numeric:
            options = [
                "Convert to Absolute", 
                "Replace with 0", 
                "Replace with Mean", 
                "Drop Rows (Negatives)"
            ]

    return options

def scan_structural_issues(df: pl.DataFrame):
    issues = []
    
    # 1. Null Check
    for col in df.columns:
        null_count = df[col].null_count()
        if null_count > 0:
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
        strats = get_fix_strategies("Duplicate Rows", pl.Object)
        issues.append({
            "type": "Duplicate Rows",
            "column": "Dataset",
            "count": dup_count,
            "dtype": "N/A",
            "options": strats
        })

    # 3. Numeric Negatives
    try:
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