"""
Cloud SQL Connection Module

This module provides a connection to Google Cloud SQL PostgreSQL database
using the Cloud SQL Python Connector. It handles connection pooling and
provides methods for obtaining connections that can be used across the application.
"""

from typing import Any, Dict, Optional
import logging
from contextlib import contextmanager

# Import the Cloud SQL Python Connector
from google.cloud.sql.connector import Connector, IPTypes

# Import settings
from .settings import settings

# You can choose either pg8000 or psycopg2 as your PostgreSQL driver
# Uncomment the one you want to use
import pg8000  # Pure Python PostgreSQL driver
# import psycopg2  # C-based PostgreSQL driver

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
            settings.db_instance_connection_name,
            "pg8000",
            user=settings.db_user,
            password=settings.db_pass,
            db=settings.db_name,
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
            settings.db_instance_connection_name,
            "psycopg2",
            user=settings.db_user,
            password=settings.db_pass,
            db=settings.db_name,
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

def execute_query(query: str, params: Optional[Any] = None, fetch: bool = True):
    """
    Execute a SQL query and optionally fetch results.

    Args:
        query (str): SQL query to execute
        params (Any, optional): Parameters for the query (can be dict for named params or tuple/list for positional)
        fetch (bool): Whether to fetch and return results

    Returns:
        list: Query results if fetch is True, else None

    Example:
        # Select query with named parameters (not supported by pg8000)
        # results = execute_query("SELECT * FROM users WHERE id = %(user_id)s", {"user_id": 1})

        # Select query with positional parameters (supported by pg8000)
        results = execute_query("SELECT * FROM users WHERE id = %s", (1,))

        # Insert/update query with positional parameters
        execute_query("INSERT INTO logs (message) VALUES (%s)", ("Test message",), fetch=False)
    """
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()

        # Set autocommit based on fetch parameter
        # For SELECT queries (fetch=True), we don't need to commit
        # For INSERT/UPDATE/DELETE queries (fetch=False), we need to commit
        connection.autocommit = False

        # Log the query (without sensitive parameters)
        query_start = query.strip().split()[0].upper() if query.strip() else "UNKNOWN"
        logger.debug(f"Executing {query_start} query")

        # Execute the query
        cursor.execute(query, params or {})

        # Fetch results if needed
        result = None
        if fetch:
            result = cursor.fetchall()
        else:
            # For non-fetch queries (INSERT/UPDATE/DELETE), commit the transaction
            connection.commit()
            logger.debug(f"{query_start} query successfully committed")

        return result
    except Exception as e:
        if connection and not fetch:
            # Rollback for non-fetch queries that might have modified data
            try:
                connection.rollback()
                logger.warning(f"Transaction rolled back due to error: {str(e)}")
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {str(rollback_error)}")

        # Include query type in error log
        query_type = query.strip().split()[0].upper() if query and query.strip() else "UNKNOWN"
        logger.error(f"Database error executing {query_type} query: {str(e)}", exc_info=True)
        raise
    finally:
        # Close cursor and connection
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logger.warning(f"Error closing cursor: {str(e)}")

        if connection:
            try:
                connection.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")

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
            # Check PostgreSQL version
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Connected to PostgreSQL: {version[0]}")

            # Check if our tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND 
                      table_name IN ('cash_recharges', 'transactions')
            """)
            tables = cursor.fetchall()
            existing_tables = [table[0] for table in tables]

            if 'cash_recharges' in existing_tables:
                logger.info("cash_recharges table exists")
            else:
                logger.warning("cash_recharges table does not exist")

            if 'transactions' in existing_tables:
                logger.info("transactions table exists")
            else:
                logger.warning("transactions table does not exist")

            # Check connection parameters
            cursor.execute("SHOW server_version;")
            server_version = cursor.fetchone()[0]

            cursor.execute("SELECT current_database();")
            current_db = cursor.fetchone()[0]

            cursor.execute("SELECT current_user;")
            current_user = cursor.fetchone()[0]

            logger.info(f"Database details - Name: {current_db}, User: {current_user}, Version: {server_version}")

            return True
    except Exception as e:
        logger.error(f"Connection test failed: {e}", exc_info=True)

        # Try to provide more diagnostic information
        try:
            # Check if we can connect at all
            conn = get_connection()
            logger.info("Basic connection succeeded but query failed")
            conn.close()
        except Exception as conn_error:
            logger.error(f"Could not establish basic connection: {str(conn_error)}")

        return False

if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)

    # Test the connection
    if test_connection():
        print("Successfully connected to the database!")
    else:
        print("Failed to connect to the database.")
