from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class BaseProvider(ABC):
    """Base class for AI providers"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the provider
        
        Args:
            api_key: API key for the provider
            config: Provider-specific configuration
        """
        self.api_key = api_key
        self.config = config
    
    @abstractmethod
    def process_message(self, 
                       system_prompt: str,
                       prompt: str,
                       content: str,
                       response_format: Optional[str] = None,
                       model: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a message using the provider's API
        
        Args:
            system_prompt: System-level instructions
            prompt: The instruction prompt
            content: The content to process
            response_format: Optional response format ('json', 'text', or dict with function definitions)
            model: Optional model override
            
        Returns:
            Dict containing:
                - processed_text: The processed result
                - model: The model used
                - tokens: Token usage information
                - processing_time: Time taken to process
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """
        Validate the provider configuration
        
        Returns:
            True if configuration is valid, False otherwise
        """
        pass