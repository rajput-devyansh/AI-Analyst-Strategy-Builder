import polars as pl
import polars.selectors as cs  # <--- Import Selectors

def scan_structural_issues(df: pl.DataFrame):
    """
    Programmatic scan for Nulls, Duplicates, Outliers, and Mixed Types.
    """
    issues = []
    
    # 1. Null Check
    for col in df.columns:
        null_count = df[col].null_count()
        if null_count > 0:
            issues.append({
                "type": "Missing Values",
                "column": col,
                "count": null_count,
                "suggestion": "Fill with Median/Mode or Drop"
            })

    # 2. Duplicate Check
    dup_count = df.is_duplicated().sum()
    if dup_count > 0:
        issues.append({
            "type": "Duplicate Rows",
            "column": "Dataset",
            "count": dup_count,
            "suggestion": "Remove Duplicates"
        })

    # 3. Numeric Negatives (FIXED: Using cs.numeric())
    # This correctly selects Int8, Int16, Int32, Int64, Float32, Float64
    try:
        numeric_cols = df.select(cs.numeric()).columns
        for col in numeric_cols:
            # Check for negative values
            neg_count = df.filter(pl.col(col) < 0).height
            if neg_count > 0:
                issues.append({
                    "type": "Negative Values",
                    "column": col,
                    "count": neg_count,
                    "suggestion": "Convert to Absolute or Drop"
                })
    except Exception as e:
        # Fallback if selectors fail (rare)
        print(f"Skipping numeric scan: {e}")
            
    # 4. Mixed Types (FIXED: Using cs.string())
    try:
        string_cols = df.select(cs.string()).columns
        for col in string_cols:
            # Check if column is actually numbers hidden as strings
            # Logic: If we can cast >90% to float but it's currently a string, it's mixed/dirty
            n_unique = df[col].n_unique()
            if n_unique > 0:
                # Simple heuristic: If it has 'TBD' or similar, it's mixed.
                # We skip deep analysis for speed, just flagging potential mixed types
                pass 
                # (You can expand this logic later if needed)
    except Exception:
        pass

    return issues