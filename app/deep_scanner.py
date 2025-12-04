import polars as pl
import json
import re
from rapidfuzz import process, fuzz
from app.llm_engine import get_llm
from typing import List, Dict, Generator

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
            pass 

        # --- PREPARE FOR VOCABULARY CHECKS ---
        n_unique = df[col].n_unique()
        
        # Only run fuzzy logic on low-to-medium cardinality columns
        if 2 < n_unique < 500:
            val_counts = df[col].drop_nulls().value_counts()
            values = val_counts[col].to_list()
            
            # --- C. CASE INCONSISTENCY ---
            lower_map = {}
            for v in values:
                l = str(v).lower()
                if l not in lower_map: lower_map[l] = []
                lower_map[l].append(v)
            
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
            total_rows = df.height
            rare_cutoff = max(1, total_rows * 0.01) # 1% threshold
            
            common = val_counts.filter(pl.col("count") > rare_cutoff)[col].to_list()
            rare = val_counts.filter(pl.col("count") <= rare_cutoff)[col].to_list()
            
            typos = []
            for r in rare:
                if not isinstance(r, str) or not r.strip(): continue
                if not common: continue
                
                match = process.extractOne(r, common, scorer=fuzz.ratio)
                if match:
                    best_match, score, _ = match
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
        if df[col].n_unique() < 5: continue 
        
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        
        if q1 is not None and q3 is not None:
            iqr = q3 - q1
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            
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
def get_batches(df: pl.DataFrame, batch_size=20) -> Generator[pl.DataFrame, None, None]:
    """Yields small SLICES of the dataframe (preserving types) instead of dicts."""
    current_idx = 0
    height = df.height
    while current_idx < height:
        # yield a DataFrame slice
        yield df.slice(current_idx, batch_size)
        current_idx += batch_size

def analyze_batch(batch_df: pl.DataFrame) -> List[Dict]:
    """
    Sends one batch to Phi-4-Mini (The Reasoner).
    Optimized to use Pipe-Separated Values for token efficiency.
    """
    llm = get_llm(mode="reasoning")
    
    # Convert DF to Pipe-Separated String (Markdown Table style)
    # This is much easier for Phi-4 to read than JSON objects
    data_str = batch_df.write_csv(separator="|")
    
    system_prompt = """
    You are a Data Logic Auditor.
    Analyze the following data sample for LOGICAL CONTRADICTIONS.
    
    COMMON TRAPS TO FIND:
    - Time Travel: 'End Date' before 'Start Date'.
    - Impossible Demographics: 'Age' < 18 but 'Status' = Married.
    - Financial Errors: 'Price' is negative, or 'Total' != Price * Qty.
    - Context Mismatch: 'City' = Paris but 'Country' = USA.
    
    OUTPUT FORMAT:
    Return a strictly valid JSON LIST of objects. 
    Format: [{"row_index": <int>, "column": "<col_name>", "issue": "<short description>"}]
    If no errors, return [].
    """
    
    user_prompt = f"""
    DATA BATCH (Pipe Separated):
    {data_str}
    
    Identify logic errors. Return JSON ONLY.
    """
    
    try:
        # Construct messages properly
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        response = llm.invoke(messages)
        content = response.content.strip()
        
        # Clean Markdown wrappers
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.replace("```", "").strip()
            
        return json.loads(content)
        
    except json.JSONDecodeError:
        # Common issue with small models, just skip batch
        return []
    except Exception as e:
        print(f"Batch Error: {e}")
        return []

def aggregate_deep_issues(raw_issues: List[Dict]) -> List[Dict]:
    """Groups individual errors into high-level insights."""
    grouped = {}
    for err in raw_issues:
        # Normalize keys to match whatever the LLM returned (case insensitive check usually good practice)
        col = err.get('column') or err.get('Col') or 'Unknown'
        issue = err.get('issue') or err.get('Issue') or 'Logic Error'
        row_idx = err.get('row_index') or err.get('Row') or '?'
        
        key = f"{col}: {issue}"
        
        if key not in grouped:
            grouped[key] = {
                "column": col, 
                "issue": issue, 
                "count": 0, 
                "rows": [],
                "type": "Logic Paradox" 
            }
        
        grouped[key]["count"] += 1
        # Only keep first 5 examples
        if len(grouped[key]["rows"]) < 5: 
            grouped[key]["rows"].append(row_idx)
            
    return list(grouped.values())