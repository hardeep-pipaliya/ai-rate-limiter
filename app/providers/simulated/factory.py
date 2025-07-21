from typing import Dict, Any

from app.config.logger import LoggerSetup
from .openai_provider import SimulatedOpenAIProvider
from .anthropic_provider import SimulatedAnthropicProvider
from .azure_provider import SimulatedAzureProvider
from .novita_provider import SimulatedNovitaProvider


# Initialize logger setup
log_setup = LoggerSetup()
logger = log_setup.setup_logger()
class SimulatedProviderFactory:
    """Factory class for creating simulated AI providers"""
    
    @staticmethod
    def create_provider(provider_name: str, api_key: str, config: Dict[str, Any]):
        """
        Create an instance of the specified simulated provider
        
        Args:
            provider_name: Name of the provider (openai, anthropic, azure, novita)
            api_key: API key for the provider (can be any string for simulation)
            config: Provider-specific configuration
            
        Returns:
            An instance of the appropriate simulated provider class
            
        Raises:
            ValueError: If provider is not supported or configuration is invalid
        """
        logger.info(f"Creating simulated provider: {provider_name} with config: {config}")
        
        providers = {
            'openai': SimulatedOpenAIProvider,
            'anthropic': SimulatedAnthropicProvider,
            'azure': SimulatedAzureProvider,
            'novita': SimulatedNovitaProvider
        }
        
        if provider_name not in providers:
            logger.error(f"Unsupported provider: {provider_name}")
            raise ValueError(f"Unsupported provider: {provider_name}")
        
        provider_class = providers[provider_name]
        logger.info(f"Selected simulated provider class: {provider_class.__name__}")
        
        try:
            provider = provider_class(api_key, config)
            logger.info(f"Successfully created simulated provider instance: {provider_class.__name__}")
        except Exception as e:
            logger.error(f"Error creating simulated provider {provider_name}: {str(e)}")
            raise
        
        if not provider.validate_config():
            logger.error(f"Invalid configuration for simulated provider {provider_name}: {config}")
            raise ValueError(f"Invalid configuration for simulated provider {provider_name}")
        
        logger.info(f"Simulated provider {provider_name} created and validated successfully")
        return provider 