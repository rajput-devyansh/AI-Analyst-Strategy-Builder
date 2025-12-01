from langchain_ollama import ChatOllama

def get_llm(mode="reasoning"):
    """
    Returns the optimal model for the specific task.
    Constraints: Optimized for RTX 3060 (6GB VRAM).
    """
    if mode == "coding":
        # The 'Hands': Writes Polars code, handles syntax, regex, and cleaning.
        return ChatOllama(
            model="qwen2.5-coder:1.5b", 
            temperature=0  # Zero temperature for precise, non-creative code
        )
    
    elif mode == "reasoning":
        # The 'Brain': Analyzes strategy, business logic, and relationships.
        return ChatOllama(
            model="cogito:3b", 
            temperature=0.2  # Low temperature for factual but articulate insights
        )
        
    else:
        # Fallback
        return ChatOllama(model="cogito:3b", temperature=0)