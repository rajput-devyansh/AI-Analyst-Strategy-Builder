import os
import requests  # We use requests to hit Ollama API for manual unloading
from langchain_ollama import ChatOllama
from typing import Literal

# --- CONFIGURATION ---
MODELS = {
    "coder": "granite4:3b",
    "reasoning": "phi4-mini-reasoning:3.8b"
}

# Track the currently loaded model to know when to switch
_current_active_mode = None
_active_llm_instance = None

def unload_model(model_name: str):
    """
    Explicitly unloads a model from VRAM using the Ollama API.
    """
    try:
        # Sending an empty prompt with keep_alive=0 unloads it immediately
        requests.post('http://localhost:11434/api/generate', json={
            "model": model_name,
            "keep_alive": 0
        })
        print(f"📉 Unloaded {model_name} from VRAM.")
    except Exception as e:
        print(f"Warning: Could not unload {model_name}: {e}")

def get_llm(mode: Literal["coding", "reasoning"] = "reasoning"):
    """
    Retrieves the LLM. 
    SAFETY FEATURE: Unloads the previous model if the mode changes 
    to prevent VRAM overflow.
    """
    global _current_active_mode, _active_llm_instance
    
    target_model = MODELS["coder"] if mode == "coding" else MODELS["reasoning"]
    
    # Check if we are switching modes (e.g., from Reasoning -> Coding)
    if _current_active_mode and _current_active_mode != mode:
        print(f"🔄 Switching modes: Unloading {_current_active_mode}...")
        
        # 1. Unload the old model from VRAM
        prev_model_name = MODELS["coder"] if _current_active_mode == "coding" else MODELS["reasoning"]
        unload_model(prev_model_name)
        
        # 2. Clear the LangChain instance
        _active_llm_instance = None

    # Instantiate if not already active
    if _active_llm_instance is None:
        print(f"🔌 Loading {mode.upper()} model: {target_model}...")
        temperature = 0.1 if mode == "coding" else 0.3
        
        _active_llm_instance = ChatOllama(
            model=target_model,
            temperature=temperature,
            keep_alive="5m" # Keep alive while we use it
        )
        _current_active_mode = mode
    
    return _active_llm_instance

if __name__ == "__main__":
    # Test the sequential switching
    print("1. Requesting Coder...")
    llm1 = get_llm("coding")
    print(llm1.invoke("print hello").content)
    
    print("\n2. Requesting Reasoner (Should trigger unload/reload)...")
    llm2 = get_llm("reasoning")
    print(llm2.invoke("Why is sky blue?").content)