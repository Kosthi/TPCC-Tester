"""
Database schema management for TPC-C benchmark.
Based on the improved design from tpcc module.
"""

import logging
from pathlib import Path
from typing import Dict
import sqlite3

from .database_connection import DatabaseConnection

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages TPC-C database schema creation and validation."""

    def __init__(self, db_connection: DatabaseConnection):
        """Initialize schema manager with database connection."""
        self.db = db_connection
        self.sql_path = Path(__file__).parent.parent / "sql"

    def create_schema(self) -> None:
        """Create TPC-C database schema from SQL files."""
        schema_file = self.sql_path / "create_tables.sql"

        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        with open(schema_file, "r") as f:
            schema_sql = f.read()

        try:
            with self.db.get_cursor() as cursor:
                cursor.executescript(schema_sql)
                logger.info("TPC-C schema created successfully")
        except sqlite3.Error as e:
            logger.error(f"Failed to create schema: {e}")
            raise

    def create_indexes(self) -> None:
        """Create performance indexes from SQL files."""
        indexes_file = self.sql_path / "create_index.sql"

        if not indexes_file.exists():
            logger.warning(f"Indexes file not found: {indexes_file}")
            return

        with open(indexes_file, "r") as f:
            indexes_sql = f.read()

        try:
            with self.db.get_cursor() as cursor:
                cursor.executescript(indexes_sql)
                logger.info("TPC-C indexes created successfully")
        except sqlite3.Error as e:
            logger.error(f"Failed to create indexes: {e}")
            raise

    def validate_schema(self) -> bool:
        """Validate that all required TPC-C tables exist."""
        required_tables = [
            "warehouse",
            "district",
            "customer",
            "item",
            "stock",
            "orders",
            "order_line",
            "new_order",
            "history",
        ]

        try:
            with self.db.get_cursor() as cursor:
                for table in required_tables:
                    cursor.execute(
                        "SELECT * FROM ?",
                        (table,),
                    )
                    if not cursor.fetchone():
                        logger.error(f"Required table '{table}' not found")
                        return False

                logger.info("Schema validation passed")
                return True

        except sqlite3.Error as e:
            logger.error(f"Schema validation failed: {e}")
            return False

    def get_table_counts(self) -> Dict[str, int]:
        """Get row counts for all TPC-C tables."""
        tables = [
            "warehouse",
            "district",
            "customer",
            "item",
            "stock",
            "orders",
            "order_line",
            "new_order",
            "history",
        ]

        counts = {}
        try:
            with self.db.get_cursor() as cursor:
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"Failed to get table counts: {e}")
            raise

        return counts

    def drop_all_tables(self) -> None:
        """Drop all TPC-C tables (for testing/cleanup)."""
        tables = [
            "warehouse",
            "district",
            "customer",
            "item",
            "stock",
            "orders",
            "order_line",
            "new_order",
            "history",
        ]

        try:
            with self.db.get_cursor() as cursor:
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info("All TPC-C tables dropped successfully")
        except sqlite3.Error as e:
            logger.error(f"Failed to drop tables: {e}")
            raise
