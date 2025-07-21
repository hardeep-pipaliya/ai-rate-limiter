from typing import Dict, Any
from .base import BaseSimulatedProvider

class SimulatedOpenAIProvider(BaseSimulatedProvider):
    """Simulated OpenAI API provider implementation"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the simulated OpenAI provider
        
        Args:
            api_key: API key (can be any string for simulation)
            config: Configuration containing:
                - model: Default model to use (e.g. "gpt-4")
        """
        super().__init__(api_key, config)
        # Add OpenAI-specific response templates
        self.response_templates.extend([
            "As GPT, I would respond: {prompt}",
            "GPT simulation analyzing your input: {content}",
            "OpenAI model {model} mock response with context: {system_prompt}"
        ])
    
    def validate_config(self) -> bool:
        """Validate the OpenAI configuration"""
        return 'model' in self.config and self.config['model'].startswith('gpt-') 