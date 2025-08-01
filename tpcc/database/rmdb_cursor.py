"""
RMDB cursor implementation for TPC-C benchmark.
Adapts the custom RMDB protocol to a cursor-like interface.
"""

import re
from typing import List, Tuple, Any, Optional, Iterator
from enum import Enum

from tpcc.database.connection import Client


class SQLState(Enum):
    SUCCESS = 7
    ABORT = 3


class RMDBCursor:
    """
    A cursor-like interface for RMDB database that mimics SQLite cursor behavior.
    Parses the custom pipe-delimited response format from RMDB.
    """

    def __init__(self, client: Client):
        """Initialize cursor with RMDB client."""
        self.client = client
        self.last_result: Optional[str] = None
        self.rows: List[Tuple] = []
        self.description: Optional[List[Tuple]] = None
        self.rowcount: int = 0
        self.arraysize: int = 1

    def execute(self, sql: str, parameters: Tuple = ()) -> None:
        """
        Execute SQL query and parse results.

        Args:
            sql: SQL query string
            parameters: Query parameters (will be substituted into SQL)
        """
        # Substitute parameters into SQL
        query = sql + ";"

        for param in parameters:
            # Handle string parameters with proper quoting and escaping
            if isinstance(param, str):
                # Escape single quotes by doubling them
                escaped_param = param.replace("'", "''")
                formatted_param = f"'{escaped_param}'"
            elif param is None:
                formatted_param = "NULL"
            else:
                formatted_param = str(param)

            query = query.replace("%s", formatted_param, 1)
            query = query.replace("?", formatted_param, 1)

        # Send query to RMDB
        result = self.client.send_cmd(query)
        self.last_result = result

        if result.startswith("abort"):
            raise Exception(f"Query aborted: {result}")

        if result.startswith("Error") or result is None or result == "":
            self.rows = []
            self.description = None
            self.rowcount = 0
            return

        # Parse the pipe-delimited format
        self._parse_result(result)

    def _parse_result(self, result: str) -> None:
        """Parse RMDB pipe-delimited response format."""
        lines = result.strip().split("\n")

        if not lines:
            self.rows = []
            return

        # Find the header line (starts with |)
        header_line = None
        for line in lines:
            if line.startswith("|"):
                header_line = line
                break

        if not header_line:
            self.rows = []
            return

        # Parse column names from header
        columns = [col.strip() for col in header_line.split("|") if col.strip()]
        self.description = [col for col in columns]

        # Parse data rows
        data_rows = []
        data_started = False

        for line in lines:
            if line.startswith("|"):
                if data_started:  # This is a data row
                    values = [val.strip() for val in line.split("|") if val.strip()]
                    if len(values) == len(columns):
                        data_rows.append(tuple(values))
                else:  # This is the header
                    data_started = True

        self.rows = data_rows
        self.rowcount = len(data_rows)

    def fetchone(self) -> Optional[Tuple]:
        """Fetch the next row of a query result set."""
        if not self.rows:
            return None
        return self.rows.pop(0)

    def fetchmany(self, size: int = None) -> List[Tuple]:
        """Fetch the next set of rows of a query result set."""
        if size is None:
            size = self.arraysize

        result = self.rows[:size]
        self.rows = self.rows[size:]
        return result

    def fetchall(self) -> List[Tuple]:
        """Fetch all remaining rows of a query result set."""
        result = self.rows
        self.rows = []
        return result

    def close(self) -> None:
        """Close the cursor."""
        self.rows = []
        self.description = None
        self.last_result = None

    def __iter__(self) -> Iterator[Tuple]:
        """Iterator protocol support."""
        return iter(self.rows)

    def __next__(self) -> Tuple:
        """Iterator protocol support."""
        if not self.rows:
            raise StopIteration
        return self.rows.pop(0)


class RMDBCursorAdapter:
    """
    Adapter class that provides SQLite-like cursor interface for RMDB.
    """

    def __init__(self, client: Client):
        self.client = client
        self._cursor = RMDBCursor(client)

    def execute(self, sql: str, parameters: Tuple = ()) -> None:
        """Execute SQL query."""
        return self._cursor.execute(sql, parameters)

    def executemany(self, sql: str, seq_of_parameters: List[Tuple]) -> None:
        """Execute SQL query with multiple parameter sets."""
        for parameters in seq_of_parameters:
            self._cursor.execute(sql, parameters)

    def executescript(self, script: str) -> None:
        """Execute a SQL script with multiple statements.

        Args:
            script: SQL script containing multiple statements separated by semicolons
        """
        # Split script into individual statements
        statements = [stmt.strip() for stmt in script.split(";") if stmt.strip()]

        for statement in statements:
            # Skip empty statements
            if not statement:
                continue

            # Execute each statement without parameters
            self._cursor.execute(statement)

    def fetchone(self) -> Optional[Tuple]:
        """Fetch one row."""
        return self._cursor.fetchone()

    def fetchmany(self, size: int = None) -> List[Tuple]:
        """Fetch many rows."""
        return self._cursor.fetchmany(size)

    def fetchall(self) -> List[Tuple]:
        """Fetch all rows."""
        return self._cursor.fetchall()

    def close(self) -> None:
        """Close cursor."""
        self._cursor.close()

    @property
    def description(self):
        """Return cursor description (column metadata)."""
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        """Return affected row count."""
        return self._cursor.rowcount

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Backward compatibility functions
class RMDBQueryExecutor:
    """
    Backward compatibility class that mimics the original sql.py functions.
    """

    def __init__(self, client: Client):
        self.client = client

    def select(self, table, col="*", where=None, order_by=None, asc=False):
        """
        Execute SELECT query with RMDB format.

        Returns:
            List of tuples for success, SQLState for abort/error
        """
        cursor = RMDBCursor(self.client)

        # Build SQL
        if isinstance(table, list):
            table_str = ",".join(table)
        else:
            table_str = table

        if isinstance(col, list):
            col_str = ",".join(col)
        else:
            col_str = col

        # Build WHERE clause
        where_clause = ""
        params = []
        if where:
            if not isinstance(where, list):
                where = [where]
            conditions = []
            for condition in where:
                if len(condition) == 3:
                    col_name, operator, value = condition
                    conditions.append(f"{col_name}{operator}%s")
                    params.append(value)
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

        # Build ORDER BY clause
        order_clause = ""
        if order_by:
            direction = "ASC" if asc else "DESC"
            order_clause = f"ORDER BY {order_by} {direction}"

        sql = f"SELECT {col_str} FROM {table_str} {where_clause} {order_clause};"

        try:
            cursor.execute(sql, tuple(params))
            return cursor.fetchall()
        except Exception as e:
            if "abort" in str(e).lower():
                return SQLState.ABORT
            return []

    def insert(self, table, rows):
        """Execute INSERT query."""
        cursor = RMDBCursor(self.client)

        if not isinstance(rows[0], list):
            rows = [rows]

        placeholders = ",".join(["%s"] * len(rows[0]))
        sql = f"INSERT INTO {table} VALUES ({placeholders});"

        try:
            for row in rows:
                cursor.execute(sql, tuple(row))
            return None
        except Exception as e:
            if "abort" in str(e).lower():
                return SQLState.ABORT
            return None

    def update(self, table, row, where=None):
        """Execute UPDATE query."""
        cursor = RMDBCursor(self.client)

        if not isinstance(row, list):
            row = [row]

        set_clause = ",".join([f"{col}=%s" for col, val in row])
        params = [val for col, val in row]

        where_clause = ""
        if where:
            if not isinstance(where, list):
                where = [where]
            conditions = []
            for condition in where:
                if len(condition) == 3:
                    col_name, operator, value = condition
                    conditions.append(f"{col_name}{operator}%s")
                    params.append(value)
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"UPDATE {table} SET {set_clause} {where_clause};"

        try:
            cursor.execute(sql, tuple(params))
            return None
        except Exception as e:
            if "abort" in str(e).lower():
                return SQLState.ABORT
            return None

    def delete(self, table, where):
        """Execute DELETE query."""
        cursor = RMDBCursor(self.client)

        where_clause = ""
        params = []
        if where:
            if not isinstance(where, list):
                where = [where]
            conditions = []
            for condition in where:
                if len(condition) == 3:
                    col_name, operator, value = condition
                    conditions.append(f"{col_name}{operator}%s")
                    params.append(value)
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"DELETE FROM {table} {where_clause};"

        try:
            cursor.execute(sql, tuple(params))
            return None
        except Exception as e:
            if "abort" in str(e).lower():
                return SQLState.ABORT
            return None
