from typing import Dict, Any

from app.config.logger import LoggerSetup
from .base import BaseProvider
from .openai_provider import OpenAIProvider
from .anthropic_provider import AnthropicProvider
from .azure_provider import AzureProvider
from .novita_provider import NovitaProvider
from .simulated import SimulatedProviderFactory
import os

# Initialize logger setup
log_setup = LoggerSetup()
logger = log_setup.setup_logger()


class ProviderFactory:
    """Factory class for creating AI providers"""
    
    @staticmethod
    def create_provider(provider_name: str, api_key: str, config: Dict[str, Any], use_simulation: bool = False) -> BaseProvider:
        """
        Create an instance of the specified provider
        
        Args:
            provider_name: Name of the provider (openai, anthropic, azure, novita)
            api_key: API key for the provider
            config: Provider-specific configuration
            use_simulation: Flag to control simulation mode from message
            
            Returns:
                An instance of the appropriate provider class with rate limiting:
                - Enforces rate limits (1000 requests per hour/minute)
                - Uses database to track request counts
                - Automatically queues requests that exceed limits
                - Resumes processing after cooldown period
            
        Raises:
            ValueError: If provider is not supported or configuration is invalid
        """
        logger.info(f"Creating provider: {provider_name} with config: {config} (Simulation mode: {use_simulation})")
        
        if use_simulation:
            logger.info("Using simulated provider")
            return SimulatedProviderFactory.create_provider(provider_name, api_key, config)
        
        providers = {
            'openai': OpenAIProvider,
            'anthropic': AnthropicProvider,
            'azure': AzureProvider,
            'novita': NovitaProvider
        }
        
        if provider_name not in providers:
            logger.error(f"Unsupported provider: {provider_name}")
            raise ValueError(f"Unsupported provider: {provider_name}")
        
        provider_class = providers[provider_name]
        logger.info(f"Selected provider class: {provider_class.__name__}")
        
        try:
            provider = provider_class(api_key, config)
            logger.info(f"Successfully created provider instance: {provider_class.__name__}")
        except Exception as e:
            logger.error(f"Error creating provider {provider_name}: {str(e)}")
            raise
        
        if not provider.validate_config():
            logger.error(f"Invalid configuration for provider {provider_name}: {config}")
            raise ValueError(f"Invalid configuration for provider {provider_name}")
        
        logger.info(f"Provider {provider_name} created and validated successfully")
        return provider 

