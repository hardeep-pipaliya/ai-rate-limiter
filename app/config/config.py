import os
from dotenv import load_dotenv

# First try to load .env, then .env.docker if .env is missing
if os.path.exists('.env'):
    load_dotenv('.env')
elif os.path.exists('.env.docker'):
    load_dotenv('.env.docker')
else:
    print("No environment file found (.env or .env.docker)")

# Load environment variables
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_API_PORT = os.getenv('RABBITMQ_API_PORT', '15672')  # Default RabbitMQ API port
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')  # Default RabbitMQ port
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')
SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL')
ARTICLE_QUEUE_SERIVCE_BASE_URL = os.getenv('ARTICLE_QUEUE_SERIVCE_BASE_URL', 'http://192.168.1.2:8501')
SQLALCHEMY_TRACK_MODIFICATIONS = False

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev')
    
    # PostgreSQL Configuration
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')