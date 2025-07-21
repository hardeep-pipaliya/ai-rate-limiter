# app/models/workers.py

from app.connection.orm_postgres_connection import db
from sqlalchemy.dialects.postgresql import JSON, UUID
import uuid
from datetime import datetime

class Worker(db.Model):
    __tablename__ = 'workers'
    
    id = db.Column(db.Integer, primary_key=True)
    slug_id = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    workspace_id = db.Column(db.Integer, db.ForeignKey('workspaces.id', ondelete='CASCADE'), nullable=False)
    pid = db.Column(db.Integer, nullable=True)  # nullable because PID may not be immediately available
    status = db.Column(db.String(50), nullable=False, default='starting')  # starting, running, stopped, failed
    worker_name = db.Column(db.String(100), nullable=False, default='default')
    worker_number = db.Column(db.Integer, nullable=True)  # <-- ✅ Added worker_number here
    log_file = db.Column(db.String(255), nullable=True)
    started_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    last_heartbeat = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    config = db.Column(JSON, nullable=False, default=dict)
    created_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now(), onupdate=datetime.utcnow)
