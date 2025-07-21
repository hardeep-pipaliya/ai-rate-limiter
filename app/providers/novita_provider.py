import time
from typing import Dict, Any, Optional
from openai import OpenAI

from app.config.logger import LoggerSetup
from .base import BaseProvider

# Initialize logger setup
log_setup = LoggerSetup()
logger = log_setup.setup_logger()

class NovitaProvider(BaseProvider):
    """Novita API provider implementation with support for Deepseek models"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the Novita provider
        
        Args:
            api_key: Novita API key
            config: Configuration containing:
                - model: Default model to use (e.g. "deepseek/deepseek_v3")
                - api_url: Novita API endpoint URL (optional)
        """
        logger.info("Initializing NovitaProvider")
        super().__init__(api_key, config)
        self.api_url = config.get('api_url', 'https://api.novita.ai/v3/openai')
        logger.info(f"Using API URL: {self.api_url}")
        
        try:
                        
            logger.info("Creating OpenAI client")
            self.client = OpenAI(
                api_key=api_key,
                base_url=self.api_url
            )
            logger.info("Successfully created OpenAI client")
            
        except Exception as e:
            logger.error(f"Error initializing NovitaProvider: {str(e)}")
            raise Exception(f"Failed to initialize NovitaProvider: {str(e)}")
        
    def validate_config(self) -> bool:
        """Validate the Novita configuration"""
        logger.info(f"Validating Novita config: {self.config}")
        is_valid = 'model' in self.config
        logger.info(f"Config validation result: {'valid' if is_valid else 'invalid'}")
        return is_valid
    
    def process_message(self,
                       system_prompt: str,
                       prompt: str,
                       content: str,
                       response_format: Optional[str] = None,
                       model: Optional[str] = None) -> Dict[str, Any]:
        """Process a message using Novita's API"""
        logger.info("Starting to process message with NovitaProvider")
        start_time = time.time()
        
        try:
            # Use provided model or default from config
            model_to_use = model or self.config['model']
            logger.info(f"Using model: {model_to_use}")
            
            # Create the messages array
            messages = []
            if system_prompt:
                messages.append({
                    "role": "system",
                    "content": system_prompt
                })
            
            # Add the instruction prompt and content
            messages.append({
                "role": "user",
                "content": f"{prompt}\n\n{content}"
            })
            logger.info(f"Prepared messages array with {len(messages)} messages")
            
            # Prepare API parameters
            api_params = {
                "model": model_to_use,
                "messages": messages,
                "max_tokens": 2048,
                "temperature": 1,
                "top_p": 1,
                "presence_penalty": 0,
                "frequency_penalty": 0,
                "extra_body": {
                    "min_p": 0,
                    "top_k": 50,
                    "repetition_penalty": 1
                }
            }
            
            # # Add response format if specified
            # if response_format:
            #     if response_format == "json":
            #         api_params["response_format"] = {"type": "json_object"}
            #     elif response_format == "text":
            #         api_params["response_format"] = {"type": "text"}
            #     # Add support for function calls if specified in response_format
            #     if "functions" in response_format:
            #         api_params["functions"] = response_format["functions"]
            #     if "function_call" in response_format:
            #         api_params["function_call"] = response_format["function_call"]
            # else:
            #     api_params["response_format"] = {"type": "text"}
            
            # Call the Novita API
            logger.info("Calling Novita API")
            response = self.client.chat.completions.create(**api_params)
            
            logger.info("Successfully received response from Novita API")
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Extract token usage
            usage = response.usage
            logger.info(f"Message processed in {processing_time:.2f}s. Tokens used: {usage.total_tokens}")
            
            # Get the response text and remove <think> content
            response_text = response.choices[0].message.content
            if "<think>" in response_text and "</think>" in response_text:
                # Extract text between </think> and end of string
                response_text = response_text.split("</think>")[-1].strip()
            
            result = {
                "processed_text": response_text,
                "model": model_to_use,
                "tokens": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens
                },
                "processing_time": processing_time
            }
            logger.info("Successfully prepared result")
            return result
            
        except Exception as e:
            logger.error(f"Novita API error: {str(e)}")
            raise Exception(f"Novita API error: {str(e)}")