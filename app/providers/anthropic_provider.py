import time
from typing import Dict, Any, Optional
import anthropic

from app.config.logger import LoggerSetup
from .base import BaseProvider

# Initialize logger setup
log_setup = LoggerSetup()
logger = log_setup.setup_logger()
class AnthropicProvider(BaseProvider):
    """Anthropic API provider implementation"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the Anthropic provider
        
        Args:
            api_key: Anthropic API key
            config: Configuration containing:
                - model: Default model to use (e.g. "claude-3-sonnet-20240229")
        """
        super().__init__(api_key, config)
        # Initialize client without any custom transport options
        logger.info(f"Initializing Anthropic client")
        self.client = anthropic.Anthropic(
            api_key=api_key
        )
        logger.info(f"Initialized Anthropic client with model config: {self.config}")
        
    def validate_config(self) -> bool:
        """Validate the Anthropic configuration"""
        return 'model' in self.config
    
    def process_message(self,
                       system_prompt: str,
                       prompt: str,
                       content: str,
                       response_format: Optional[str] = None,
                       model: Optional[str] = None) -> Dict[str, Any]:
        """Process a message using Anthropic's API"""
        start_time = time.time()
        
        try:
            # Use provided model or default from config
            model_to_use = model or self.config['model']
            logger.info(f"Processing message with model: {model_to_use}")
            
            # Create messages array with just the user message
            messages = [{
                "role": "user",
                "content": f"{prompt}\n\n{content}"
            }]
            
            # Prepare API parameters
            api_params = {
                "model": model_to_use,
                "max_tokens": 2000,
                "system": system_prompt if system_prompt else "",
                "messages": messages
            }
            
            # # Add response format if specified
            # if response_format:
            #     if response_format == "json":
            #         # For Anthropic, we need to add instructions in the system prompt
            #         json_instruction = "Respond using only valid JSON."
            #         api_params["system"] = (api_params["system"] + " " + json_instruction).strip()
            #     # Note: Anthropic doesn't support function calling in the same way as OpenAI
            
            logger.info(f"Sending request to Anthropic API with system prompt and user message")
            
            # Call the Anthropic API
            response = self.client.messages.create(**api_params)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            result = {
                "processed_text": response.content[0].text,
                "model": model_to_use,
                "tokens": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                    "total_tokens": response.usage.input_tokens + response.usage.output_tokens
                },
                "processing_time": processing_time
            }
            
            logger.info(f"Successfully processed message in {processing_time:.2f} seconds")
            return result
            
        except Exception as e:
            logger.error(f"Anthropic API error: {str(e)}")
            raise Exception(f"Anthropic API error: {str(e)}")