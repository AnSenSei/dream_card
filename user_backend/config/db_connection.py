"""
Cloud SQL Connection Module

This module provides a connection to Google Cloud SQL PostgreSQL database
using the Cloud SQL Python Connector. It handles connection pooling and
provides methods for obtaining connections that can be used across the application.
"""

from typing import Any, Dict, Optional
import os
import logging
from contextlib import contextmanager

# Import the Cloud SQL Python Connector
from google.cloud.sql.connector import Connector, IPTypes

# You can choose either pg8000 or psycopg2 as your PostgreSQL driver
# Uncomment the one you want to use
import pg8000  # Pure Python PostgreSQL driver
# import psycopg2  # C-based PostgreSQL driver

# Constants from environment or hardcoded for development
# In production, use environment variables for sensitive data
INSTANCE_CONNECTION_NAME = os.getenv(
    "DB_INSTANCE_CONNECTION_NAME", 
    "seventh-program-433718-h8:us-central1:test"
)
DB_USER = os.getenv("DB_USER", "test")  # Replace with your actual username
DB_PASS = os.getenv("DB_PASS", "Qjh19981201!")  # Replace with your actual password
DB_NAME = os.getenv("DB_NAME", "test")      # Replace with your actual database name
DB_PORT = int(os.getenv("DB_PORT", "5432"))

# Set up logging
logger = logging.getLogger(__name__)

# Initialize the connector
connector = Connector()

def get_connection_pg8000():
    """
    Returns a database connection using pg8000 driver.
    
    Returns:
        pg8000.dbapi.Connection: A database connection object
    """
    try:
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            ip_type=IPTypes.PUBLIC  # Use PUBLIC since we're connecting to a public IP
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise

# Alternative function if you prefer to use psycopg2
def get_connection_psycopg2():
    """
    Returns a database connection using psycopg2 driver.
    
    Returns:
        psycopg2.extensions.connection: A database connection object
    """
    try:
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "psycopg2",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            ip_type=IPTypes.PUBLIC  # Use PUBLIC since we're connecting to a public IP
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise

# Use pg8000 by default (can be changed based on your preference)
get_connection = get_connection_pg8000

@contextmanager
def db_connection():
    """
    Context manager for database connections.
    
    Yields:
        Connection: A database connection that will be automatically closed.
    
    Example:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM your_table")
            results = cursor.fetchall()
    """
    conn = None
    try:
        conn = get_connection()
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

@contextmanager
def db_cursor(commit=False):
    """
    Context manager for database cursors.
    
    Args:
        commit (bool): Whether to commit the transaction when exiting.
    
    Yields:
        Cursor: A database cursor that will be automatically closed.
    
    Example:
        with db_cursor(commit=True) as cursor:
            cursor.execute("INSERT INTO your_table VALUES (%s, %s)", [value1, value2])
    """
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            if commit:
                conn.rollback()
            logger.error(f"Database cursor error: {e}")
            raise
        finally:
            cursor.close()

def execute_query(query: str, params: Optional[Dict[str, Any]] = None, fetch: bool = True):
    """
    Execute a SQL query and optionally fetch results.
    
    Args:
        query (str): SQL query to execute
        params (dict, optional): Parameters for the query
        fetch (bool): Whether to fetch and return results
    
    Returns:
        list: Query results if fetch is True, else None
    
    Example:
        # Select query
        results = execute_query("SELECT * FROM users WHERE id = %(user_id)s", {"user_id": 1})
        
        # Insert/update query
        execute_query("INSERT INTO logs (message) VALUES (%(message)s)", {"message": "Test"}, fetch=False)
    """
    with db_cursor(commit=not fetch) as cursor:
        cursor.execute(query, params or {})
        if fetch:
            return cursor.fetchall()
        return None

def close_connector():
    """
    Close the Cloud SQL connector when shutting down the application.
    Should be called during application shutdown.
    """
    connector.close()

# Test connection function
def test_connection():
    """
    Test the database connection by executing a simple query.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        with db_cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Connected to PostgreSQL: {version[0]}")
            return True
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False

if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Test the connection
    if test_connection():
        print("Successfully connected to the database!")
    else:
        print("Failed to connect to the database.")
