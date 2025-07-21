import time
from typing import Dict, Any, Optional
from openai import OpenAI
from .base import BaseProvider

class OpenAIProvider(BaseProvider):
    """OpenAI API provider implementation"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the OpenAI provider
        
        Args:
            api_key: OpenAI API key
            config: Configuration containing:
                - model: Default model to use
        """
        super().__init__(api_key, config)
        self.client = OpenAI(
            api_key=api_key,
            http_client=None  # Use default client without proxy settings
        )
        
    def validate_config(self) -> bool:
        """Validate the OpenAI configuration"""
        return 'model' in self.config
    
    def process_message(self,
                       system_prompt: str,
                       prompt: str,
                       content: str,
                       response_format: Optional[str] = None,
                       model: Optional[str] = None) -> Dict[str, Any]:
        """Process a message using OpenAI's API"""
        start_time = time.time()
        
        try:
            # Use provided model or default from config
            model_to_use = model or self.config['model']
            
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
            
            # Prepare API parameters
            api_params = {
                "model": model_to_use,
                "messages": messages,
                "temperature": 0.7,
                "max_tokens": 2000,
                "top_p": 1.0,
                "frequency_penalty": 0.0,
                "presence_penalty": 0.0
            }
            
            # Add response format if specified
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
            
            # Call the OpenAI API
            response = self.client.chat.completions.create(**api_params)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Extract token usage
            usage = response.usage
            
            return {
                "processed_text": response.choices[0].message.content,
                "model": model_to_use,
                "tokens": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens
                },
                "processing_time": processing_time
            }
            
        except Exception as e:
            raise Exception(f"OpenAI API error: {str(e)}")