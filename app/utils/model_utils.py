from typing import Optional, Tuple
from app.models.provider_keys import ProviderKey  # Adjust the import if needed
from sqlalchemy.orm import Session

from app.models.workspaces import WorkSpace

def get_provider_for_model(model: str, workspace_id: int, db_session: Session) -> Tuple[Optional[str], Optional[str]]:
    """
    Get the provider name for a given model by checking the ProviderKey table.

    Args:
        model: Model name
        workspace_id: Workspace ID
        db_session: SQLAlchemy database session

    Returns:
        Tuple[Optional[str], Optional[str]]: (provider_name, error_message)
        - If model is found and available: (provider_name, None)
        - If model is not found: (None, error_message)
    """
    try:
        workspace = WorkSpace.query.filter_by(workerspace_id=workspace_id).first()
        if not workspace:
            return ...

        # Query all provider keys for the workspace
        provider_keys = db_session.query(ProviderKey).filter_by(workspace_id=workspace.id).all()
        
        if not provider_keys:
            return None, f"No provider keys found for workspace {workspace_id}"
        
        # Check if any provider has the requested model
        for provider_key in provider_keys:
            if provider_key.model == model:
                return provider_key, None
        
        return None, f"Model '{model}' is not configured for any provider in workspace {workspace_id}"
    
    except Exception as e:
        return None, str(e)
