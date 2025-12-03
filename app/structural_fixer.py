import polars as pl

def apply_fix(df: pl.DataFrame, fix_type: str, column: str = None, custom_val=None) -> pl.DataFrame:
    """
    Executes the fix. Handles Custom Values, Modes, and Drops safely.
    """
    
    # --- HANDLING CUSTOM VALUE INPUT ---
    if fix_type == "Fill with Custom Value":
        if column and df[column].dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
            try:
                custom_val = float(custom_val)
                if custom_val.is_integer(): custom_val = int(custom_val)
            except:
                pass 
        return df.with_columns(pl.col(column).fill_null(custom_val))
    
    if fix_type == "Replace Negatives with Custom Value":
        if df[column].dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
            try:
                custom_val = float(custom_val)
            except:
                pass
        return df.with_columns(
            pl.when(pl.col(column) < 0).then(custom_val).otherwise(pl.col(column)).alias(column)
        )

    # --- 1. MISSING VALUE FIXES ---
    if fix_type == "Fill with Median":
        return df.with_columns(pl.col(column).fill_null(pl.col(column).median()))

    elif fix_type == "Fill with Mean":
        return df.with_columns(pl.col(column).fill_null(pl.col(column).mean()))

    elif fix_type == "Fill with Mode":
        try:
            mode_series = df[column].drop_nulls().mode()
            if mode_series.len() > 0:
                mode_val = mode_series.item(0)
                if mode_val is not None:
                    return df.with_columns(pl.col(column).fill_null(mode_val))
            return df 
        except Exception:
            return df

    elif fix_type == "Fill with 0":
        return df.with_columns(pl.col(column).fill_null(0))
        
    elif fix_type == "Fill with -1":
        return df.with_columns(pl.col(column).fill_null(-1))

    elif fix_type == "Fill with 'Unknown'":
        return df.with_columns(pl.col(column).fill_null("Unknown"))
        
    elif fix_type == "Fill with 'Missing'":
        return df.with_columns(pl.col(column).fill_null("Missing"))

    elif fix_type == "Forward Fill" or "Forward Fill" in fix_type:
        return df.with_columns(pl.col(column).fill_null(strategy="forward"))
    
    elif fix_type == "Backward Fill" or "Backward Fill" in fix_type:
        return df.with_columns(pl.col(column).fill_null(strategy="backward"))

    elif fix_type == "Drop Rows (Nulls)":
        return df.drop_nulls(subset=[column])

    # --- 2. NEGATIVE VALUE FIXES ---
    elif fix_type == "Convert to Absolute":
        return df.with_columns(pl.col(column).abs())

    elif fix_type == "Replace with 0" or fix_type == "Set Negatives to 0":
        return df.with_columns(
            pl.when(pl.col(column) < 0).then(0).otherwise(pl.col(column)).alias(column)
        )
        
    elif fix_type == "Replace with Mean":
        mean_val = df[column].mean()
        return df.with_columns(
            pl.when(pl.col(column) < 0).then(mean_val).otherwise(pl.col(column)).alias(column)
        )

    # --- CRITICAL FIX: PRESERVE NULLS WHEN DROPPING NEGATIVES ---
    elif fix_type == "Drop Rows (Negatives)":
        # Keep values that are >= 0 OR are Null
        # (Because "Missing Values" is a separate issue type we handle elsewhere)
        return df.filter((pl.col(column) >= 0) | pl.col(column).is_null())

    # --- 3. DUPLICATE FIXES ---
    elif fix_type == "Remove Duplicates":
        return df.unique()
        
    elif fix_type == "Keep First":
        return df.unique(keep='first')
        
    elif fix_type == "Keep Last":
        return df.unique(keep='last')

    return df