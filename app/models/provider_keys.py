from sqlalchemy.dialects.postgresql import JSON , UUID
import uuid
from app.connection.orm_postgres_connection import db
from sqlalchemy.ext.hybrid import hybrid_property


class ProviderKey(db.Model):
    __tablename__ = 'provider_keys'
    
    id = db.Column(db.Integer, primary_key=True)
    slug_id = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    workspace_id = db.Column(db.Integer, db.ForeignKey('workspaces.id', ondelete='CASCADE'), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    api_key = db.Column(db.String(255), nullable=False)
    rate_limit = db.Column(db.Integer, nullable=False, default=1000)  # Default rate limit
    rate_limit_period = db.Column(db.String(50), nullable=False, default='hour')  # Default rate limit period
    rate_limit_period_value = db.Column(db.Integer, nullable=False, default=1)  # Default to 1 (e.g. 1 hour, 1 day)
    config = db.Column(JSON, nullable=False, default=lambda: {
        'model': 'gpt-3.5-turbo',
        'api_version': 'v1',
        'endpoint': 'https://api.openai.com/v1/chat/completions'
    })  # Default endpoint
    created_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now())

    # Add relationship to WorkSpace
    workspace = db.relationship('WorkSpace', backref=db.backref('provider_keys', lazy=True))

    @hybrid_property
    def model(self):
        return self.config.get('model')
    
    @model.setter
    def model(self, value):
        if not isinstance(value, str):
            raise ValueError('Model must be a string')
        self.config['model'] = value

    @hybrid_property
    def api_version(self):
        return self.config.get('api_version')
    
    @api_version.setter
    def api_version(self, value):
        if not isinstance(value, str):
            raise ValueError('API Version must be a string')
        self.config['api_version'] = value

    @hybrid_property
    def endpoint(self):
        return self.config.get('endpoint')
    
    @endpoint.setter
    def endpoint(self, value):
        if not isinstance(value, str):
            raise ValueError('Endpoint must be a string')
        self.config['endpoint'] = value

    @hybrid_property
    def rate_limit_window(self):
        """Get the full rate limit window description"""
        return f"{self.rate_limit_period_value} {self.rate_limit_period}"