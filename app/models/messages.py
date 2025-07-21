from app.connection.orm_postgres_connection import db
from sqlalchemy import Enum
from sqlalchemy.dialects.postgresql import JSON, UUID
import uuid
from enum import Enum as PythonEnum


class MessageStatus(PythonEnum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    FAILED = 'failed'
    SUCCESS = 'success'

class Message(db.Model):
    __tablename__ = 'messages'
    
    id = db.Column(db.Integer, primary_key=True)
    slug_id = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    article_id = db.Column(UUID(as_uuid=True), nullable=True)
    article_message_total_count = db.Column(db.Integer, nullable=True, default=0)
    workspace_id = db.Column(db.Integer, db.ForeignKey('workspaces.id', ondelete='CASCADE'), nullable=False)
    token_count = db.Column(db.Integer, nullable=True, default=1)
    provider_key_id = db.Column(db.Integer, db.ForeignKey('provider_keys.id', ondelete='CASCADE'), nullable=False)
    sequence_index = db.Column(db.Integer, nullable=True, default=0)
    status = db.Column(
        Enum(MessageStatus, name='message_status', 
        values_callable=lambda x: [e.value for e in x]),
        nullable=False, 
        default=MessageStatus.PENDING.value
    )
    message = db.Column(db.Text, nullable=False)
    prompt = db.Column(db.Text, nullable=True)
    html_tag = db.Column(db.Text, nullable=True)
    request = db.Column(JSON, nullable=False)
    result = db.Column(JSON, nullable=False)
    message_priority = db.Column(db.Integer, nullable=True, default=0)
    message_field_type = db.Column(db.String(50), nullable=True)
    status_code = db.Column(db.Integer, nullable=False, default=200)
    created_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now(), onupdate=db.func.now())