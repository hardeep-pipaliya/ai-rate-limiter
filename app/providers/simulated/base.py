import time
import random
import json
from typing import Dict, Any, Optional
from ..base import BaseProvider

class BaseSimulatedProvider(BaseProvider):
    """Base class for simulated AI providers"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the simulated provider
        
        Args:
            api_key: API key (can be any string for simulation)
            config: Provider-specific configuration
        """
        super().__init__(api_key, config)
        self.response_templates = [
            "This is a simulated response from {model}. The system prompt was: {system_prompt}",
            "Here's a mock response based on your input: {content}",
            "Simulated {model} response to your prompt: {prompt}",
            "I am a simulated version of {model}, processing your request with system context: {system_prompt}",
            "Mock AI response from {model} considering your input: {content}"
        ]
        
        self.error_templates = [
            "Rate limit exceeded for {model}. Please try again later.",
            "Invalid request format for {model}. Check your input parameters.",
            "Model {model} is currently overloaded. Please try again.",
            "Connection timeout while accessing {model}. Please retry.",
            "Internal server error occurred while processing request for {model}.",
            "Token limit exceeded for {model}. Try reducing your input size.",
            "Authentication failed for {model}. Check your API key.",
            "Model {model} is currently unavailable. Please try another model."
        ]
    
    def _generate_token_count(self) -> Dict[str, int]:
        """Generate realistic-looking token counts"""
        prompt_tokens = random.randint(100, 500)
        completion_tokens = random.randint(50, 200)
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens
        }
    
    def _simulate_processing_time(self) -> float:
        """Simulate realistic processing time between 1-5 seconds"""
        processing_time = random.uniform(1, 5)
        time.sleep(processing_time)
        return processing_time
    
    def _should_simulate_error(self) -> bool:
        """Randomly determine if we should simulate an error (10% chance)"""
        return random.random() < 0.10
    
    def _generate_error_response(self, model: str) -> str:
        """Generate a simulated error message"""
        template = random.choice(self.error_templates)
        return template.format(model=model)
    
    def _generate_response(self, system_prompt: str, prompt: str, content: str, model: str) -> str:
        """Generate a simulated response using templates"""
        template = random.choice(self.response_templates)
        return template.format(
            model=model,
            system_prompt=system_prompt,
            prompt=prompt,
            content=content
        )
    
    def process_message(self,
                       system_prompt: str,
                       prompt: str,
                       content: str,
                       response_format: Optional[str] = None,
                       model: Optional[str] = None) -> Dict[str, Any]:
        """Process a message using simulated response"""
        # Use provided model or default from config
        model_to_use = model or self.config['model']
        
        # Simulate processing time
        start_time = time.time()
        processing_time = self._simulate_processing_time()
        
        # Check if we should simulate an error
        if self._should_simulate_error():
            error_message = self._generate_error_response(model_to_use)
            raise Exception(error_message)
        
        # Generate simulated response using templates - the AI provider should handle the format
        response_text = self._generate_response(
            system_prompt,
            prompt,
            content,
            model_to_use
        )
        
        # Generate token usage
        token_usage = self._generate_token_count()
        
        return {
            "processed_text": response_text,
            "model": model_to_use,
            "tokens": token_usage,
            "processing_time": processing_time
        }