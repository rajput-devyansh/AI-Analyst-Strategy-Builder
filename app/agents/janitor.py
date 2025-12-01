import re
import polars as pl
from app.llm_engine import get_llm

def run_janitor(profile, issue_context, user_instruction):
    llm = get_llm(mode="coding")
    max_retries = 3
    last_error = ""
    
    base_prompt = f"""
    You are a Polars Python Expert.
    PROFILE: {profile}
    ISSUE: {issue_context}
    COMMAND: {user_instruction}
    
    TASK: Write a function `clean_data(df)` that takes/returns a Polars DataFrame.
    RULES: 
    1. Use 'import polars as pl'.
    2. Handle types strictly (cast strings to numbers if needed).
    3. Return ONLY python code inside ```python``` blocks.
    4. If removing rows, use `df.filter(...)`.
    5. If modifying columns, use `df.with_columns(...)`.
    """

    for attempt in range(max_retries):
        prompt = base_prompt
        if last_error:
            prompt += f"\n\n⚠️ PREVIOUS CODE FAILED: {last_error}\nFIX IT."
            
        response = llm.invoke(prompt)
        
        # Extract Code
        match = re.search(r"```python(.*?)```", response.content, re.DOTALL)
        code = match.group(1).strip() if match else response.content
        
        # Test Compile
        try:
            local_env = {'pl': pl}
            exec(code, globals(), local_env)
            if 'clean_data' in local_env:
                return code # Success!
        except Exception as e:
            last_error = str(e)
            
    return f"# Error: Could not generate valid code. {last_error}"