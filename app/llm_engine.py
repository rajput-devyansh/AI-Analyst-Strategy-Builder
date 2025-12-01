from langchain_ollama import ChatOllama

def get_llm(mode="reasoning"):
    if mode == "coding":
        # Qwen 2.5 Coder: Best small coding model
        return ChatOllama(model="qwen2.5-coder:1.5b", temperature=0)
    
    elif mode == "reasoning":
        # Cogito 3B: Excellent at detecting logic contradictions
        return ChatOllama(model="cogito:3b", temperature=0.1)
        
    return ChatOllama(model="cogito:3b", temperature=0)