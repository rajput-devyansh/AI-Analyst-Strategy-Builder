import polars as pl
import json
from app.llm_engine import get_llm

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
            grouped[key] = {"column": err.get('Col'), "issue": err.get('Issue'), "count": 0, "rows": []}
        grouped[key]["count"] += 1
        if len(grouped[key]["rows"]) < 5: grouped[key]["rows"].append(err.get('Row'))
    return list(grouped.values())