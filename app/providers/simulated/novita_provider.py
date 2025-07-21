from typing import Dict, Any
from .base import BaseSimulatedProvider

class SimulatedNovitaProvider(BaseSimulatedProvider):
    """Simulated Novita API provider implementation with support for DeepSeek models"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the simulated Novita provider
        
        Args:
            api_key: API key (can be any string for simulation)
            config: Configuration containing:
                - model: Default model to use (e.g. "deepseek/deepseek-r1-turbo")
                - api_url: Novita API endpoint URL (optional)
        """
        super().__init__(api_key, config)
        # Add DeepSeek-specific response templates
        self.response_templates.extend([
            "DeepSeek simulation processing: {prompt}",
            "Novita API mock response using {model}: {content}",
            "Simulated DeepSeek model analyzing with context: {system_prompt}"
        ])
    
    def validate_config(self) -> bool:
        """Validate the Novita configuration"""
        return 'model' in self.config and 'deepseek' in self.config['model'].lower() 