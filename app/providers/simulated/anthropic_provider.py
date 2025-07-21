from typing import Dict, Any
from .base import BaseSimulatedProvider

class SimulatedAnthropicProvider(BaseSimulatedProvider):
    """Simulated Anthropic API provider implementation"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the simulated Anthropic provider
        
        Args:
            api_key: API key (can be any string for simulation)
            config: Configuration containing:
                - model: Default model to use (e.g. "claude-3-sonnet-20240229")
        """
        super().__init__(api_key, config)
        # Add Claude-specific response templates
        self.response_templates.extend([
            "Claude simulation responding: {prompt}",
            "As a simulated Claude model, I analyze: {content}",
            "Claude {model} mock response considering: {system_prompt}"
        ])
    
    def validate_config(self) -> bool:
        """Validate the Anthropic configuration"""
        return 'model' in self.config and 'claude' in self.config['model'].lower() 