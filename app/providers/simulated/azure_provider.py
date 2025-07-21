from typing import Dict, Any
from .base import BaseSimulatedProvider

class SimulatedAzureProvider(BaseSimulatedProvider):
    """Simulated Azure OpenAI API provider implementation"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the simulated Azure provider
        
        Args:
            api_key: API key (can be any string for simulation)
            config: Configuration containing:
                - model: Azure model/deployment name
                - api_version: Azure OpenAI API version
                - endpoint: Azure endpoint URL
        """
        super().__init__(api_key, config)
        # Add Azure-specific response templates
        self.response_templates.extend([
            "Azure OpenAI simulation ({model}) responding: {prompt}",
            "Azure deployment mock response: {content}",
            "Simulated Azure model with endpoint {endpoint} considering: {system_prompt}"
        ])
    
    def validate_config(self) -> bool:
        """Validate the Azure configuration"""
        required_fields = ['model', 'api_version', 'endpoint']
        return all(field in self.config for field in required_fields)
    
    def _generate_response(self, system_prompt: str, prompt: str, content: str, model: str) -> str:
        """Override to include Azure endpoint in templates"""
        template = super()._generate_response(system_prompt, prompt, content, model)
        return template.format(
            model=model,
            system_prompt=system_prompt,
            prompt=prompt,
            content=content,
            endpoint=self.config.get('endpoint', 'unknown-endpoint')
        ) 