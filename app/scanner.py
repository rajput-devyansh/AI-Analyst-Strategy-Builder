import polars as pl
import polars.selectors as cs

def get_fix_strategies(issue_type, dtype, col_name):
    """
    Returns a dictionary with:
    - 'rec': The best practice fix (Recommended)
    - 'alts': A list of alternative valid fixes
    """
    strategies = {
        "rec": "",
        "alts": []
    }
    
    # 1. MISSING VALUES (NULLS)
    if issue_type == "Missing Values":
        if dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
            strategies["rec"] = "Fill with Median (Robust)"
            strategies["alts"] = ["Fill with Mean", "Fill with 0", "Drop Rows", "Fill with Mode"]
        else: # Strings/Dates
            strategies["rec"] = "Fill with 'Unknown'"
            strategies["alts"] = ["Drop Rows", "Fill with Mode (Most Frequent)"]

    # 2. DUPLICATES
    elif issue_type == "Duplicate Rows":
        strategies["rec"] = "Remove Duplicates"
        strategies["alts"] = ["Keep All (Do Nothing)"]

    # 3. NEGATIVE VALUES (Numeric)
    elif issue_type == "Negative Values":
        strategies["rec"] = "Convert to Absolute (Positive)"
        strategies["alts"] = ["Replace with 0", "Drop Rows", "Multiply by -1"]

    return strategies

def scan_structural_issues(df: pl.DataFrame):
    """
    Scans for issues and attaches SMART STRATEGIES to them.
    """
    issues = []
    
    # 1. Null Check
    for col in df.columns:
        null_count = df[col].null_count()
        if null_count > 0:
            strats = get_fix_strategies("Missing Values", df[col].dtype, col)
            issues.append({
                "type": "Missing Values",
                "column": col,
                "count": null_count,
                "strategies": strats # <--- New Field
            })

    # 2. Duplicate Check
    dup_count = df.is_duplicated().sum()
    if dup_count > 0:
        strats = get_fix_strategies("Duplicate Rows", None, "Dataset")
        issues.append({
            "type": "Duplicate Rows",
            "column": "Dataset",
            "count": dup_count,
            "strategies": strats
        })

    # 3. Numeric Negatives
    try:
        numeric_cols = df.select(cs.numeric()).columns
        for col in numeric_cols:
            neg_count = df.filter(pl.col(col) < 0).height
            if neg_count > 0:
                strats = get_fix_strategies("Negative Values", df[col].dtype, col)
                issues.append({
                    "type": "Negative Values",
                    "column": col,
                    "count": neg_count,
                    "strategies": strats
                })
    except Exception:
        pass
            
    return issues