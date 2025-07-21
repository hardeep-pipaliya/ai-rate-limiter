import time
from typing import Dict, Any, Optional
from openai import AzureOpenAI
from .base import BaseProvider

class AzureProvider(BaseProvider):
    """Azure OpenAI API provider implementation"""
    
    def __init__(self, api_key: str, config: Dict[str, Any]):
        """
        Initialize the Azure OpenAI provider
        
        Args:
            api_key: Azure API key
            config: Configuration containing:
                - model: Azure model/deployment name
                - api_version: Azure OpenAI API version
                - endpoint: Azure endpoint URL
        """
        super().__init__(api_key, config)
        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=config['api_version'],
            azure_endpoint=config['endpoint'],
            http_client=None  # Use default client without proxy settings
        )
        
    def validate_config(self) -> bool:
        """Validate the Azure configuration"""
        required_fields = ['model', 'api_version', 'endpoint']
        return all(field in self.config for field in required_fields)
    
    def process_message(self,
                       system_prompt: str,
                       prompt: str,
                       content: str,
                       response_format: Optional[str] = None,
                       model: Optional[str] = None) -> Dict[str, Any]:
        """Process a message using Azure OpenAI API"""
        start_time = time.time()
        
        try:
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
                "model": model or self.config['model'],
                "messages": messages,
                "temperature": 0.7,
                "max_tokens": 2000,
                "top_p": 1.0,
                "frequency_penalty": 0.0,
                "presence_penalty": 0.0
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
            
            # Call the Azure OpenAI API
            response = self.client.chat.completions.create(**api_params)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Extract token usage
            usage = response.usage
            
            return {
                "processed_text": response.choices[0].message.content,
                "model": model or self.config['model'],
                "tokens": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens
                },
                "processing_time": processing_time
            }
            
        except Exception as e:
            raise Exception(f"Azure OpenAI API error: {str(e)}")