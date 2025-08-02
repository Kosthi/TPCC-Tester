"""
Database connection management for TPC-C benchmark.
Refactored from the original connection.py to provide better abstraction.
Supports custom RMDB protocol with pipe-delimited response format.
"""

import logging
from contextlib import contextmanager
from typing import Optional

from tpcc.database.connection import Client
from tpcc.database.rmdb_cursor import RMDBCursorAdapter

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages database connections with proper resource management for RMDB."""

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        """Initialize database connection.

        Args:
            host: RMDB server host
            port: RMDB server port
        """
        self.host = host
        self.port = port
        self.client: Optional[Client] = None
        self._connected = False

    def connect(self) -> None:
        """Establish database connection to RMDB."""
        try:
            self.client = Client()
            self._connected = True
            logger.info("Connected to RMDB database")
        except Exception as e:
            logger.error(f"Failed to connect to RMDB: {e}")
            raise

    def close(self) -> None:
        """Close database connection."""
        if self.client:
            self.client.close()
            self.client = None
            self._connected = False
            logger.info("RMDB connection closed")

    @contextmanager
    def get_cursor(self):
        """Context manager for RMDB cursor."""
        if not self.client:
            raise RuntimeError("Database not connected")

        cursor = RMDBCursorAdapter(self.client)
        try:
            yield cursor
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            cursor.close()

    def execute_query(self, query: str, params: tuple = ()) -> list:
        """Execute a SELECT query and return results."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def execute_update(self, query: str, params: tuple = ()) -> int:
        """Execute an INSERT/UPDATE/DELETE query and return affected rows."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount
        except Exception as e:
            logger.error(f"Update execution failed: {e}")
            raise

    def execute_script(self, sql_script: str) -> None:
        """Execute multiple SQL statements."""
        try:
            with self.get_cursor() as cursor:
                cursor.executescript(sql_script)
        except Exception as e:
            logger.error(f"Script execution failed: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def is_connected(self) -> bool:
        """Check if database connection is active."""
        return self._connected
