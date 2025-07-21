import psycopg2
from psycopg2.pool import SimpleConnectionPool
from typing import Optional
import os
from contextlib import contextmanager
from dotenv import load_dotenv

# First try to load .env, then .env.docker if .env is missing
load_dotenv()
class PostgresConnection:
    _instance = None
    _pool = None
    
    def __init__(self):
        """
        Initialize PostgreSQL connection pool
        """
        if PostgresConnection._pool is None:
            self._create_pool()
    
    @classmethod
    def get_instance(cls) -> 'PostgresConnection':
        """
        Get singleton instance of PostgresConnection
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _create_pool(self) -> None:
        """
        Create connection pool with environment variables
        """
        db_config = {
            'dbname': os.getenv('POSTGRES_DB', 'postgres'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }
        
        PostgresConnection._pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **db_config
        )
    
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool with context manager support
        Usage:
            with PostgresConnection.get_instance().get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM users")
                    rows = cur.fetchall()
        """
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                self._pool.putconn(conn)
    
    def execute_query(self, query: str, parameters: tuple = None) -> list:
        """
        Execute a query and return results
        :param query: SQL query string
        :param parameters: Query parameters as tuple
        :return: List of query results
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, parameters)
                if cur.description:  # If query returns results
                    return cur.fetchall()
                return []
    
    def execute_many(self, query: str, parameters: list) -> None:
        """
        Execute same query with multiple sets of parameters
        :param query: SQL query string
        :param parameters: List of parameter tuples
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, parameters)
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database
        :param table_name: Name of the table to check
        :return: True if table exists, False otherwise
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """
        result = self.execute_query(query, (table_name,))
        return result[0][0] if result else False

# Usage Example:
# db = PostgresConnection.get_instance()
# results = db.execute_query("SELECT * FROM users WHERE id = %s", (user_id,))