"""
Test script for RMDB cursor functionality.
Demonstrates how to use the new RMDB cursor with the TPC-C benchmark.
"""

import sys
import os

# Add tpcc to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tpcc.database.database_connection import DatabaseConnection
from tpcc.database.rmdb_cursor import RMDBQueryExecutor


def test_rmdb_connection():
    """Test basic RMDB connection and cursor functionality."""
    print("Testing RMDB cursor functionality...")

    try:
        # Test 1: Basic connection
        print("\n1. Testing database connection...")
        with DatabaseConnection() as db:
            print("✓ Database connection established")

            # Test 2: Cursor creation
            print("\n2. Testing cursor creation...")
            with db.get_cursor() as cursor:
                print("✓ Cursor created successfully")

                # Test 3: Simple SELECT query
                print("\n3. Testing SELECT query...")
                try:
                    cursor.execute("SELECT * FROM warehouse WHERE w_id = 1")
                    result = cursor.fetchall()
                    print(f"✓ SELECT query executed, returned {len(result)} rows")
                    if result:
                        print(f"  First row: {result[0]}")
                except Exception as e:
                    print(f"  SELECT test failed (expected if no data): {e}")

                # Test 4: INSERT query
                print("\n4. Testing INSERT query...")
                try:
                    cursor.execute(
                        "INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            666,
                            "'Thouse'",
                            "'123 Main St'",
                            "'456 Main St'",
                            "'Test City'",
                            "'CA'",
                            "'12345'",
                            0.1,
                            1000.0,
                        ),
                    )
                    print("✓ INSERT query executed")
                except Exception as e:
                    print(
                        f"  INSERT test failed (expected if table doesn't exist): {e}"
                    )

                # Test 5: Using RMDBQueryExecutor for compatibility
                print("\n5. Testing RMDBQueryExecutor...")
                executor = RMDBQueryExecutor(db.client)
                try:
                    # Test with warehouse table
                    result = executor.select("warehouse", where=[("w_id", "=", 1)])
                    if isinstance(result, list):
                        print(f"✓ RMDBQueryExecutor SELECT returned {len(result)} rows")
                    else:
                        print(f"  Query returned: {result}")
                except Exception as e:
                    print(f"  RMDBQueryExecutor test failed: {e}")

    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

    return True


def test_cursor_methods():
    """Test individual cursor methods."""
    print("\n" + "=" * 50)
    print("Testing cursor methods...")

    try:
        with DatabaseConnection() as db:
            with db.get_cursor() as cursor:
                # Test fetchone
                print("\n1. Testing fetchone...")
                cursor.execute("SELECT * FROM warehouse WHERE w_id = 1")
                row = cursor.fetchone()
                print(f"   fetchone result: {row}")

                # Test fetchmany
                print("\n2. Testing fetchmany...")
                cursor.execute("SELECT * FROM warehouse")
                rows = cursor.fetchmany(5)
                print(f"   fetchmany(5) returned {len(rows)} rows")

                # Test fetchall
                print("\n3. Testing fetchall...")
                cursor.execute("SELECT * FROM warehouse")
                all_rows = cursor.fetchall()
                print(f"   fetchall returned {len(all_rows)} rows")

                # Test description
                print("\n4. Testing cursor description...")
                cursor.execute("SELECT * FROM warehouse")
                if cursor.description:
                    print(f"   Description: {cursor.description}")
                else:
                    print("   No description available")

    except Exception as e:
        print(f"Cursor methods test failed: {e}")
        return False

    return True


def test_parameter_substitution():
    """Test parameter substitution in queries."""
    print("\n" + "=" * 50)
    print("Testing parameter substitution...")

    try:
        with DatabaseConnection() as db:
            with db.get_cursor() as cursor:
                # Test with different parameter formats
                print("\n1. Testing with %s format...")
                cursor.execute("SELECT * FROM warehouse WHERE w_id = %s", (1,))
                result = cursor.fetchall()
                print(f"   Result: {len(result)} rows")

                print("\n2. Testing with ? format...")
                cursor.execute("SELECT * FROM warehouse WHERE w_id = ?", (1,))
                result = cursor.fetchall()
                print(f"   Result: {len(result)} rows")

                print("\n3. Testing string parameter with quotes...")
                cursor.execute(
                    "SELECT * FROM warehouse WHERE w_name = %s", ("Test Warehouse",)
                )
                result = cursor.fetchall()
                print(f"   String parameter test: {len(result)} rows")

                print("\n4. Testing string parameter with single quotes...")
                cursor.execute(
                    "SELECT * FROM warehouse WHERE w_name = %s", ("O'Reilly's Store",)
                )
                result = cursor.fetchall()
                print(f"   Escaped quotes test: {len(result)} rows")

    except Exception as e:
        print(f"Parameter substitution test failed: {e}")
        return False

    return True


def test_executescript():
    """Test executescript functionality."""
    print("\n" + "=" * 50)
    print("Testing executescript...")

    try:
        with DatabaseConnection() as db:
            with db.get_cursor() as cursor:
                # Test executescript with multiple statements
                script = """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    value INTEGER
                );
                INSERT INTO test_table VALUES (1, 'test1', 100);
                INSERT INTO test_table VALUES (2, 'test2', 200);
                INSERT INTO test_table VALUES (3, 'O''Reilly''s Store', 300);
                SELECT * FROM test_table;
                """

                print("\n1. Testing executescript with DDL and DML...")
                try:
                    cursor.executescript(script)
                    print("   ✓ executescript completed successfully")

                    # Verify the data was inserted
                    cursor.execute("SELECT * FROM test_table")
                    results = cursor.fetchall()
                    print(f"   ✓ Found {len(results)} rows in test_table")
                    for row in results:
                        print(f"     Row: {row}")

                except Exception as e:
                    print(f"   executescript test failed: {e}")

                # Test with simple statements
                print("\n2. Testing executescript with simple statements...")
                simple_script = """
                DELETE FROM test_table WHERE id = 1;
                UPDATE test_table SET value = 300 WHERE id = 2;
                """
                try:
                    cursor.executescript(simple_script)
                    print("   ✓ Simple executescript completed")

                    # Verify the changes
                    cursor.execute("SELECT * FROM test_table")
                    results = cursor.fetchall()
                    print(f"   ✓ After update: {len(results)} rows")
                    for row in results:
                        print(f"     Row: {row}")

                except Exception as e:
                    print(f"   Simple executescript test failed: {e}")

    except Exception as e:
        print(f"Executescript test failed: {e}")
        return False

    return True


if __name__ == "__main__":
    print("RMDB Cursor Test Suite")
    print("=" * 50)

    # Run tests
    success = True
    success &= test_rmdb_connection()
    success &= test_cursor_methods()
    success &= test_parameter_substitution()
    success &= test_executescript()

    print("\n" + "=" * 50)
    if success:
        print("✓ All tests completed successfully!")
    else:
        print("✗ Some tests failed. Check the output above for details.")

    print("\nUsage examples:")
    print("1. Basic usage:")
    print("   from tpcc.database.database_connection import DatabaseConnection")
    print("   with DatabaseConnection() as db:")
    print("       with db.get_cursor() as cursor:")
    print("           cursor.execute('SELECT * FROM warehouse WHERE w_id = 1')")
    print("           results = cursor.fetchall()")
    print()
    print("2. Compatibility usage:")
    print("   from tpcc.database.rmdb_cursor import RMDBQueryExecutor")
    print("   executor = RMDBQueryExecutor(client)")
    print("   results = executor.select('warehouse', where=[('w_id', '=', 1)])")
