from app.llm_engine import get_llm

def run_architect(profile):
    llm = get_llm(mode="reasoning")
    
    prompt = f"""
    You are a Senior Data Architect. Review this dataset profile carefully.
    
    DATA PROFILE:
    {profile}
    
    YOUR TASK:
    1. Identify the Domain (Retail, Finance, Healthcare, etc.).
    2. Identify the Granularity (What does 1 row represent?).
    3. LIST 3 DATA QUALITY ISSUES you can see (e.g., suspicious nulls, weird formats, potential outliers).
    4. Suggest a Strategy Question this data could answer.
    
    Be concise. Use Emojis.
    """
    
    response = llm.invoke(prompt)
    return response.content