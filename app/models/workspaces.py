from app.connection.orm_postgres_connection import db
from sqlalchemy.dialects.postgresql import JSON , UUID
import uuid
class WorkSpace(db.Model):
    __tablename__ = 'workspaces'
    
    id = db.Column(db.Integer, primary_key=True)
    slug_id = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    workerspace_id = db.Column(db.String(100), nullable=False, unique=True)
    created_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_date = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


