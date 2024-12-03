import threading
import google.generativeai as genai
from typing import Dict, Optional

class LLMService:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-flash")
        self.contexts: Dict[str, str] = {}
        self.contexts_lock = threading.Lock()
        
    def create_context(self, context_id: str) -> bool:
        """Create a new empty context."""
        with self.contexts_lock:
        with self.contexts_lock:
            if context_id in self.contexts:
                return False
            self.contexts[context_id] = ""
            return True
        
    def add_query_to_context(self, context_id: str, query: str) -> bool:
        """Add a query to a context without generating response."""
        with self.contexts_lock:
            if context_id not in self.contexts:
                return False
                
            if self.contexts[context_id]:
                self.contexts[context_id] += "\n"
            self.contexts[context_id] += f"Query: {query}"
            return True
            
    def generate_response(self, context_id: str) -> Optional[str]:
        """Generate LLM response for the current context."""
        with self.contexts_lock:
            if context_id not in self.contexts:
                return None
            
            prompt = self.contexts[context_id] + "\nAnswer: "
            response = self.model.generate_content(prompt)
            return response.text
        
    def save_answer(self, context_id: str, answer: str) -> bool:
        """Save a selected answer to the context."""
        with self.contexts_lock:
            if context_id not in self.contexts:
                return False
                
            self.contexts[context_id] += f"\nAnswer: {answer}"
            return True
        
    def get_context(self, context_id: str) -> Optional[str]:
        """Retrieve a specific context."""
        with self.contexts_lock:
            return self.contexts.get(context_id)
        
    def get_all_contexts(self) -> Dict[str, str]:
        """Retrieve all contexts."""
        with self.contexts_lock:
            return self.contexts.copy()