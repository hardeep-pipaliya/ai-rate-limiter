from datetime import datetime
from typing import Any, Dict

def format_response(data: Any = None, message: str = None, status: bool = True) -> Dict:
    """
    Format API response consistently
    """
    return {
        "status": status,
        "message": message,
        "data": data,
        "timestamp": datetime.utcnow().isoformat()
    }

def validate_input(data: Dict, required_fields: list) -> tuple[bool, str]:
    """
    Validate if all required fields are present in the input data
    """
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    return True, "Validation successful"