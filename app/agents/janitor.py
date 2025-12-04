import re
import polars as pl
from app.llm_engine import get_llm
from langchain_core.messages import SystemMessage, HumanMessage

def run_janitor(profile: str, issue_context: str, user_instruction: str) -> str:
    """
    Uses the 'coding' model (Granite) to generate a robust Polars cleaning function.
    """
    llm = get_llm(mode="coding")
    max_retries = 3
    last_error = ""

    # 1. System Prompt: Defines the persona and strict constraints
    system_instruction = """
    You are a Senior Python Data Engineer specializing in the Polars library.
    
    YOUR TASK:
    Write a Python function named `clean_data(df: pl.DataFrame) -> pl.DataFrame`.
    
    CONSTRAINTS:
    1. USE ONLY POLARS. Do not use Pandas.
    2. Input and Output must be a Polars DataFrame.
    3. Handle data types strictly (e.g., cast str to int before math).
    4. NO EXPLANATIONS. NO MARKDOWN TEXT. ONLY CODE.
    5. Wrap code in ```python ... ``` block.
    6. Ensure all imports are inside the function or assume 'import polars as pl' exists.
    """

    # 2. User Context: The variable data
    user_context = f"""
    DATA PROFILE (Schema & Stats):
    {profile}

    DETECTED ISSUE:
    {issue_context}

    USER COMMAND:
    {user_instruction}
    
    Generate the `clean_data` function now.
    """

    for attempt in range(max_retries):
        messages = [
            SystemMessage(content=system_instruction),
            HumanMessage(content=user_context)
        ]

        if last_error:
            messages.append(HumanMessage(content=f"⚠️ PREVIOUS CODE FAILED WITH ERROR: {last_error}\nFIX THE SYNTAX AND RE-GENERATE."))

        # Invoke Granite
        try:
            response = llm.invoke(messages)
            raw_content = response.content
        except Exception as e:
            return f"# Error: LLM connection failed. {str(e)}"

        # 3. Robust Extraction
        # Tries to find python block. If missing, assumes whole text is code (risky but needed for local models)
        match = re.search(r"```python(.*?)```", raw_content, re.DOTALL)
        if match:
            code = match.group(1).strip()
        else:
            # Fallback: strip generic markdown if code block is missing
            code = raw_content.replace("```", "").strip()

        # 4. Validation (The "Compiler" Check)
        try:
            local_env = {'pl': pl}
            # We explicitly allow the code to run to check for syntax errors
            exec(code, globals(), local_env)
            
            if 'clean_data' not in local_env:
                raise ValueError("Function `clean_data` was not found in generated code.")
            
            # If we get here, syntax is valid
            return code

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            last_error = str(e)
            
    return f"# Error: Failed to generate valid code after {max_retries} attempts.\n# Last Error: {last_error}"