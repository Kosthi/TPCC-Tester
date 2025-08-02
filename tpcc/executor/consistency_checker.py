"""
Consistency Check Executor for TPC-C benchmark.
Provides comprehensive data validation and statistics collection.
"""

import logging
from typing import Dict, Any, List

from ..database.database_connection import DatabaseConnection

logger = logging.getLogger(__name__)


class ConsistencyCheckExecutor:
    """Dedicated executor for TPC-C data consistency validation and statistics."""

    # TPC-C constants
    DISTRICTS_PER_WAREHOUSE = 10
    ITEMS_TOTAL = 100000

    def __init__(self, db_connection: DatabaseConnection, scale_factor: int = 1):
        """Initialize Consistency Check Executor.

        Args:
            db_connection: Database connection instance
            scale_factor: Number of warehouses in the test
        """
        self.db = db_connection
        self.scale_factor = scale_factor

    def run_consistency_checks(self) -> Dict[str, bool]:
        """Run comprehensive consistency checks on loaded TPC-C data.

        Returns:
            Dictionary mapping check names to their pass/fail status
        """
        logger.info("Running consistency checks...")

        checks = {}

        # Basic count validations
        checks.update(self._check_table_counts())

        # Advanced consistency checks
        # checks.update(self._check_district_order_consistency())

        logger.info("Consistency checks completed")
        return checks

    def _check_table_counts(self) -> Dict[str, bool]:
        """Check table row counts against expected values."""
        checks = {}

        # Define the check configuration for all tables
        table_checks = [
            ("warehouse", self.scale_factor),
            ("district", self.scale_factor * self.DISTRICTS_PER_WAREHOUSE),
            ("item", self.ITEMS_TOTAL),
            (
                "customer",
                self.scale_factor * self.DISTRICTS_PER_WAREHOUSE * 3000,
            ),  # 3000 customers per district
            (
                "stock",
                self.scale_factor * self.ITEMS_TOTAL,
            ),  # One stock record per item per warehouse
            (
                "orders",
                self.scale_factor * self.DISTRICTS_PER_WAREHOUSE * 3000,
            ),  # One order per customer
            (
                "order_line",
                self.scale_factor * self.DISTRICTS_PER_WAREHOUSE * 3000 * 10,
            ),  # Average of 10 lines per order
            (
                "new_orders",
                self.scale_factor * self.DISTRICTS_PER_WAREHOUSE * 900,
            ),  # 900 new orders per district
            (
                "history",
                self.scale_factor * self.DISTRICTS_PER_WAREHOUSE * 3000,
            ),  # One history record per customer
        ]

        for table_name, expected in table_checks:
            check_name = f"{table_name}_count"
            try:
                result = self.db.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                actual = int(result[0][0])
                checks[check_name] = actual == expected
                logger.info(
                    f"{table_name.capitalize()} count: {actual}/{expected} {'✓' if checks[check_name] else '✗'}"
                )
            except Exception as e:
                checks[check_name] = False
                logger.error(f"{table_name.capitalize()} count check failed: {e}")

        return checks

    def _check_district_order_consistency(self) -> Dict[str, bool]:
        """Check district next order ID consistency."""
        checks = {"district_order_consistency": True}

        try:
            result = self.db.execute_query("""
                SELECT d_w_id, d_id, d_next_o_id
                FROM district
                ORDER BY d_w_id, d_id
            """)

            for d_w_id, d_id, d_next_o_id in result:
                max_order = self.db.execute_query(
                    """
                    SELECT MAX(o_id)
                    FROM orders
                    WHERE o_w_id = ? AND o_d_id = ?
                    """,
                    (d_w_id, d_id),
                )[0][0]

                max_new_order = self.db.execute_query(
                    """
                    SELECT MAX(no_o_id)
                    FROM new_orders
                    WHERE no_w_id = ? AND no_d_id = ?
                    """,
                    (d_w_id, d_id),
                )[0][0]

                expected = 3001  # Initial next_o_id value
                if max_order:
                    expected = max_order + 1

                if d_next_o_id != expected:
                    logger.warning(
                        f"District {d_w_id}-{d_id}: next_o_id={d_next_o_id}, expected={expected}"
                    )
                    checks["district_order_consistency"] = False

        except Exception as e:
            checks["district_order_consistency"] = False
            logger.error(f"District consistency check failed: {e}")

        return checks

    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics.

        Returns:
            Dictionary mapping table names to their row counts
        """
        logger.info("Collecting database statistics...")

        stats = {}
        tables = [
            "warehouse",
            "district",
            "customer",
            "item",
            "stock",
            "orders",
            "order_line",
            "new_orders",
            "history",
        ]

        for table in tables:
            try:
                result = self.db.execute_query(f"SELECT COUNT(*) FROM {table}")
                stats[table] = result[0][0]
                logger.debug(f"Table {table}: {stats[table]} rows")
            except Exception as e:
                logger.error(f"Failed to count {table}: {e}")
                stats[table] = 0

        logger.info("Database statistics collected")
        return stats

    def validate_data_integrity(self) -> Dict[str, bool]:
        """Perform advanced data integrity validation.

        Returns:
            Dictionary with detailed validation results
        """
        logger.info("Performing data integrity validation...")

        validations = {}

        # Check foreign key relationships
        validations.update(self._validate_foreign_keys())

        # Check data consistency rules
        validations.update(self._validate_business_rules())

        logger.info("Data integrity validation completed")
        return validations

    def _validate_foreign_keys(self) -> Dict[str, bool]:
        """Validate foreign key relationships."""
        checks = {}

        try:
            # Check warehouse references in district
            result = self.db.execute_query("""
                SELECT COUNT(*) FROM district d
                WHERE NOT EXISTS (
                    SELECT 1 FROM warehouse w WHERE w.w_id = d.d_w_id
                )
            """)
            checks["district_warehouse_fk"] = result[0][0] == 0

            # Check district references in customer
            result = self.db.execute_query("""
                SELECT COUNT(*) FROM customer c
                WHERE NOT EXISTS (
                    SELECT 1 FROM district d 
                    WHERE d.d_w_id = c.c_w_id AND d.d_id = c.c_d_id
                )
            """)
            checks["customer_district_fk"] = result[0][0] == 0

            # Check warehouse references in stock
            result = self.db.execute_query("""
                SELECT COUNT(*) FROM stock s
                WHERE NOT EXISTS (
                    SELECT 1 FROM warehouse w WHERE w.w_id = s.s_w_id
                )
            """)
            checks["stock_warehouse_fk"] = result[0][0] == 0

            # Check item references in stock
            result = self.db.execute_query("""
                SELECT COUNT(*) FROM stock s
                WHERE NOT EXISTS (
                    SELECT 1 FROM item i WHERE i.i_id = s.s_i_id
                )
            """)
            checks["stock_item_fk"] = result[0][0] == 0

        except Exception as e:
            logger.error(f"Foreign key validation failed: {e}")
            checks["foreign_key_validation"] = False

        return checks

    def _validate_business_rules(self) -> Dict[str, bool]:
        """Validate TPC-C business rules."""
        checks = {}

        try:
            # Check that all warehouses have exactly 10 districts
            result = self.db.execute_query("""
                SELECT w_id, COUNT(*) as district_count
                FROM district
                GROUP BY w_id
                HAVING district_count != 10
            """)
            checks["warehouse_district_count"] = len(result) == 0

            # Check that all districts have customers
            result = self.db.execute_query("""
                SELECT d_w_id, d_id
                FROM district d
                WHERE NOT EXISTS (
                    SELECT 1 FROM customer c 
                    WHERE c.c_w_id = d.d_w_id AND c.c_d_id = d.d_id
                )
            """)
            checks["district_has_customers"] = len(result) == 0

        except Exception as e:
            logger.error(f"Business rule validation failed: {e}")
            checks["business_rule_validation"] = False

        return checks
