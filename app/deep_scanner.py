import polars as pl
import json
import re
from rapidfuzz import process, fuzz
from app.llm_engine import get_llm

# ==============================================================================
# ENGINE 1: VOCABULARY & PATTERN SCANNER (Code-Based / Fast)
# ==============================================================================
def scan_vocabulary_issues(df: pl.DataFrame):
    """
    Checks String columns for:
    1. Invalid Email Formats (Missing '@')
    2. Encoding Artifacts (Mojibake)
    3. Case Inconsistency ("ny" vs "NY")
    4. Typos (Fuzzy Matching)
    """
    issues = []
    # Select only string columns
    str_cols = df.select(pl.col(pl.String)).columns
    
    for col in str_cols:
        
        # --- A. EMAIL FORMAT CHECK (Regex) ---
        if "email" in col.lower():
            # Check for non-null strings that DO NOT contain '@'
            bad_emails = df.filter(
                (pl.col(col).is_not_null()) & 
                (~pl.col(col).str.contains("@"))
            )
            if bad_emails.height > 0:
                issues.append({
                    "type": "Invalid Email Format (Missing @)",
                    "column": col,
                    "count": bad_emails.height,
                    "examples": bad_emails[col].head(3).to_list(),
                    "suggestion": "Drop rows or Fix manually"
                })

        # --- B. ENCODING CHECK (Garbage Chars) ---
        # Heuristic: looking for common UTF-8 decoding mishaps like 'Ã©'
        try:
            garbage_rows = df.filter(pl.col(col).str.contains(r"[ÃÂ€â]"))
            if garbage_rows.height > 0:
                issues.append({
                    "type": "Encoding Artifacts (Mojibake)",
                    "column": col,
                    "count": garbage_rows.height,
                    "examples": garbage_rows[col].head(3).to_list(),
                    "suggestion": "Fix Encoding or Replace Characters"
                })
        except:
            pass # Regex might fail on some binary strings

        # --- PREPARE FOR VOCABULARY CHECKS ---
        # Get counts of unique values (excluding nulls)
        n_unique = df[col].n_unique()
        
        # Only run fuzzy logic on low-to-medium cardinality columns (Categories, Cities)
        # Skipping IDs/UUIDs (> 500 uniques) to save time
        if 2 < n_unique < 500:
            val_counts = df[col].drop_nulls().value_counts()
            values = val_counts[col].to_list()
            
            # --- C. CASE INCONSISTENCY ---
            # Map lowercase -> list of original values
            lower_map = {}
            for v in values:
                l = str(v).lower()
                if l not in lower_map: lower_map[l] = []
                lower_map[l].append(v)
            
            # Find buckets where the same word appears in multiple cases (e.g. ['ny', 'NY'])
            inconsistent = [vals for vals in lower_map.values() if len(vals) > 1]
            if inconsistent:
                issues.append({
                    "type": "Inconsistent Case",
                    "column": col,
                    "count": len(inconsistent),
                    "examples": [f"{x[0]}/{x[1]}" for x in inconsistent[:3]],
                    "suggestion": "Normalize to Title Case or Upper Case"
                })

            # --- D. TYPO DETECTION (Fuzzy Logic) ---
            # Strategy: Compare RARE items (< 1% freq) to COMMON items (> 1% freq)
            total_rows = df.height
            rare_cutoff = max(1, total_rows * 0.01) # 1% threshold
            
            common = val_counts.filter(pl.col("count") > rare_cutoff)[col].to_list()
            rare = val_counts.filter(pl.col("count") <= rare_cutoff)[col].to_list()
            
            typos = []
            for r in rare:
                if not isinstance(r, str) or not r.strip(): continue
                if not common: continue
                
                # Use RapidFuzz to find the closest match in the 'common' list
                match = process.extractOne(r, common, scorer=fuzz.ratio)
                if match:
                    best_match, score, _ = match
                    # Threshold: 85% similarity (High confidence typo)
                    if 85 <= score < 100:
                        typos.append(f"'{r}' -> '{best_match}'")
            
            if typos:
                issues.append({
                    "type": "Potential Typos",
                    "column": col,
                    "count": len(typos),
                    "examples": typos[:3],
                    "suggestion": "Map typos to common values"
                })

    return issues

# ==============================================================================
# ENGINE 2: STATISTICAL SCANNER (Code-Based / Fast)
# ==============================================================================
def scan_statistical_issues(df: pl.DataFrame):
    """
    Checks Numeric and Date columns for:
    1. Statistical Outliers (IQR Method)
    2. Date Anomalies (Year Range 1900-2030)
    """
    issues = []
    
    # --- A. NUMERIC OUTLIERS (IQR) ---
    num_cols = df.select(pl.col([pl.Int64, pl.Float64, pl.Int32, pl.Float32])).columns
    for col in num_cols:
        # Skip binary columns (0/1) or very low cardinality numbers
        if df[col].n_unique() < 5: continue 
        
        # Calculate IQR
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        
        if q1 is not None and q3 is not None:
            iqr = q3 - q1
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            
            # Find rows outside bounds
            outliers = df.filter((pl.col(col) < lower_bound) | (pl.col(col) > upper_bound))
            if outliers.height > 0:
                issues.append({
                    "type": "Statistical Outliers (IQR)",
                    "column": col,
                    "count": outliers.height,
                    "examples": outliers[col].head(3).to_list(),
                    "suggestion": "Cap values (Winsorize) or Drop rows"
                })

    # --- B. DATE RANGES ---
    date_cols = df.select(pl.col([pl.Date, pl.Datetime])).columns
    for col in date_cols:
        # Check for Years < 1900 or > 2030
        weird_dates = df.filter(
            (pl.col(col).dt.year() < 1900) | (pl.col(col).dt.year() > 2030)
        )
        if weird_dates.height > 0:
            issues.append({
                "type": "Suspicious Year (Outside 1900-2030)",
                "column": col,
                "count": weird_dates.height,
                "examples": weird_dates[col].dt.year().head(3).to_list(),
                "suggestion": "Filter out-of-bounds dates"
            })
            
    return issues

# ==============================================================================
# ENGINE 3: LOGIC SCANNER (AI Batching / Slow)
# ==============================================================================
def get_batches(df: pl.DataFrame, batch_size=20):
    """Yields small chunks of data with row indices."""
    for i in range(0, df.height, batch_size):
        slice_df = df.slice(i, batch_size)
        yield slice_df.with_row_index(name="Row_Index", offset=i).to_dicts()

def analyze_batch(batch_data):
    """Sends one batch to Cogito 3B."""
    llm = get_llm(mode="reasoning")
    data_str = json.dumps(batch_data, default=str)
    
    prompt = f"""
    You are a Data Logic Auditor. Review these {len(batch_data)} rows.
    DATA: {data_str}
    
    TASK: Find LOGICAL contradictions or BUSINESS rule violations.
    
    LOOK FOR THESE SPECIFIC TRAPS:
    - Dates: Arrival before Ship? Exit before Join? Future dates (2099)?
    - Demographics: Age 5 Married? Age 150?
    - Finance: Credit Score > 800 Rejected? Income 0?
    - Math: Total != Price * Qty?
    - Geography: City/Country mismatch?
    
    OUTPUT: Return a strictly valid JSON LIST of objects. If no errors, return [].
    Format: [{{"Row": 12, "Col": "Age", "Issue": "Child Marriage detected", "Fix": "Filter Age < 18"}}]
    """
    try:
        response = llm.invoke(prompt)
        content = response.content.strip()
        # Clean Markdown wrapper if present
        if "```json" in content: content = content.split("```json")[1].split("```")[0]
        elif "```" in content: content = content.split("```")[1].split("```")[0]
        return json.loads(content)
    except:
        return []

def aggregate_deep_issues(raw_issues):
    """Groups individual errors into high-level insights."""
    grouped = {}
    for err in raw_issues:
        key = f"{err.get('Col')}: {err.get('Issue')}"
        if key not in grouped:
            grouped[key] = {
                "column": err.get('Col'), 
                "issue": err.get('Issue'), 
                "count": 0, 
                "rows": [],
                "type": "Logic Paradox" # UI Helper
            }
        grouped[key]["count"] += 1
        if len(grouped[key]["rows"]) < 5: grouped[key]["rows"].append(err.get('Row'))
    return list(grouped.values())