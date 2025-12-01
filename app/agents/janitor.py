import re
from app.llm_engine import get_llm

def run_janitor(profile, instruction):
    llm = get_llm(mode="coding")
    
    prompt = f"""
    You are an Expert Python Data Engineer.
    
    DATA PROFILE:
    {profile}
    
    USER REQUEST:
    "{instruction}"
    
    TASK:
    Write a Python function `clean_data(df)` using the POLARS library (`import polars as pl`) to perform the requested cleaning.
    
    RULES:
    1. Input: `df` (pl.DataFrame). Output: `df` (pl.DataFrame).
    2. USE ONLY POLARS SYNTAX. Do not use Pandas.
    3. Handle data types carefully (e.g., convert strings to numbers if needed).
    4. Return ONLY the code inside ```python ``` blocks.
    """
    
    response = llm.invoke(prompt)
    
    # Extract code block using Regex
    match = re.search(r"```python(.*?)```", response.content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return response.content