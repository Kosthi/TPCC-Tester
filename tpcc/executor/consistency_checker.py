"""
Consistency Check Executor for TPC-C benchmark.
Provides comprehensive data validation and statistics collection.
"""

import logging
from typing import Dict, Any

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
        checks.update(self._check_district_order_consistency())
        checks.update(self._check_new_orders_consistency())
        checks.update(self._check_order_line_consistency())

        # Summary of results
        passed = sum(1 for v in checks.values() if v)
        total = len(checks)

        logger.info(f"Consistency checks completed: {passed}/{total} checks passed")

        if passed == total:
            logger.info("All consistency checks passed! ✓")
        else:
            failed_checks = [k for k, v in checks.items() if not v]
            logger.warning(f"Failed checks: {failed_checks}")

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
                result = self.db.execute_query(
                    f"SELECT COUNT(*) as count FROM {table_name}"
                )
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
            warehouse_count = self.scale_factor

            for w_id in range(1, warehouse_count + 1):
                for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                    # Get district next_o_id for this specific warehouse and district
                    district_result = self.db.execute_query(
                        "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
                        (w_id, d_id),
                    )

                    if not district_result:
                        logger.warning(f"District {w_id}-{d_id} not found")
                        checks["district_order_consistency"] = False
                        continue

                    d_next_o_id = int(district_result[0][0])

                    # Get max order ID for this warehouse and district
                    max_order_result = self.db.execute_query(
                        """
                        SELECT MAX(o_id) as max_o_id
                        FROM orders
                        WHERE o_w_id = ?
                          AND o_d_id = ?
                        """,
                        (w_id, d_id),
                    )

                    if not max_order_result:
                        logger.warning(f"Orders {w_id}-{d_id} not found")
                        checks["district_order_consistency"] = False
                        continue

                    max_o_id = int(max_order_result[0][0])

                    # Get max new order ID for this warehouse and district
                    max_new_order_result = self.db.execute_query(
                        """
                        SELECT MAX(no_o_id) as max_no_o_id
                        FROM new_orders
                        WHERE no_w_id = ?
                          AND no_d_id = ?
                        """,
                        (w_id, d_id),
                    )

                    if not max_new_order_result:
                        logger.warning(f"New orders {w_id}-{d_id} not found")
                        checks["district_order_consistency"] = False
                        continue

                    max_no_o_id = int(max_new_order_result[0][0])

                    # Check consistency: d_next_o_id - 1 should equal max_o_id and max_no_o_id
                    expected = max_o_id + 1
                    consistent = (
                        d_next_o_id - 1 == max_o_id and d_next_o_id - 1 == max_no_o_id
                    )

                    if not consistent:
                        logger.warning(
                            f"District {w_id}-{d_id}: d_next_o_id={d_next_o_id}, "
                            f"max_o_id={max_o_id}, max_no_o_id={max_no_o_id}, "
                            f"expected_next_o_id={expected}"
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

    def _check_new_orders_consistency(self) -> Dict[str, bool]:
        """Check consistency for new_orders table."""
        checks = {"new_orders_consistency": True}

        try:
            warehouse_count = self.scale_factor

            for w_id in range(1, warehouse_count + 1):
                for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                    # Get new_orders count for this specific warehouse and district
                    count_new_orders_result = self.db.execute_query(
                        "SELECT COUNT(no_o_id) as count_no_o_id FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                        (w_id, d_id),
                    )

                    if not count_new_orders_result:
                        logger.warning(
                            f"New orders count_no_o_id with {w_id}-{d_id} not found"
                        )
                        checks["new_orders_consistency"] = False
                        continue

                    count_no_o_id = int(count_new_orders_result[0][0])

                    # Get max order ID for this warehouse and district
                    max_new_orders_result = self.db.execute_query(
                        """
                        SELECT MAX(no_o_id) as max_no_o_id
                        FROM new_orders
                        WHERE no_w_id = ?
                          AND no_d_id = ?
                        """,
                        (w_id, d_id),
                    )

                    if not max_new_orders_result:
                        logger.warning(
                            f"New orders max_no_o_id with {w_id}-{d_id} not found"
                        )
                        checks["new_orders_consistency"] = False
                        continue

                    max_no_o_id = int(max_new_orders_result[0][0])

                    # Get min new order ID for this warehouse and district
                    min_new_orders_result = self.db.execute_query(
                        """
                        SELECT MIN(no_o_id) as min_no_o_id
                        FROM new_orders
                        WHERE no_w_id = ?
                          AND no_d_id = ?
                        """,
                        (w_id, d_id),
                    )

                    if not min_new_orders_result:
                        logger.warning(
                            f"New orders min_no_o_id with {w_id}-{d_id} not found"
                        )
                        checks["new_orders_consistency"] = False
                        continue

                    min_no_o_id = int(min_new_orders_result[0][0])

                    # Check consistency: new_orders - min_no_o_id + 1 should equal count_no_o_id
                    expected = max_no_o_id - min_no_o_id + 1
                    consistent = count_no_o_id == expected

                    if not consistent:
                        logger.warning(
                            f"New orders {w_id}-{d_id}: count_no_o_id={count_no_o_id}, "
                            f"max_no_o_id={max_no_o_id}, min_no_o_id={min_no_o_id}, "
                            f"expected_count_no_o_id={expected}"
                        )
                        checks["new_orders_consistency"] = False

        except Exception as e:
            checks["new_orders_consistency"] = False
            logger.error(f"New orders consistency check failed: {e}")

        return checks

    def _check_order_line_consistency(self) -> Dict[str, bool]:
        """Check orders order line data consistency."""
        checks = {"order_line_consistency": True}

        try:
            warehouse_count = self.scale_factor

            for w_id in range(1, warehouse_count + 1):
                for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                    # Get sum(o_ol_cnt) from orders for this specific warehouse and district
                    sum_orders_result = self.db.execute_query(
                        "SELECT SUM(o_ol_cnt) as sum_o_ol_cnt FROM orders WHERE o_w_id = ? AND o_d_id = ?",
                        (w_id, d_id),
                    )

                    if not sum_orders_result:
                        logger.warning(f"Orders {w_id}-{d_id} not found")
                        checks["order_line_consistency"] = False
                        continue

                    sum_o_ol_cnt = int(sum_orders_result[0][0])

                    # Get count from order_line for this warehouse and district
                    count_order_line_result = self.db.execute_query(
                        """
                        SELECT COUNT(ol_o_id) as count_ol_o_id
                        FROM order_line
                        WHERE ol_w_id = ?
                          AND ol_d_id = ?
                        """,
                        (w_id, d_id),
                    )

                    if not count_order_line_result:
                        logger.warning(f"Order line {w_id}-{d_id} not found")
                        checks["order_line_consistency"] = False
                        continue

                    count_ol_o_id = int(count_order_line_result[0][0])

                    # Check consistency: sum_o_ol_cnt should equal count_ol_o_id
                    expected = count_ol_o_id
                    consistent = sum_o_ol_cnt == count_ol_o_id

                    if not consistent:
                        logger.warning(
                            f"Order line {w_id}-{d_id}: sum_o_ol_cnt={sum_o_ol_cnt}, "
                            f"count_ol_o_id={count_ol_o_id}, "
                            f"expected_sum_o_ol_cnt={expected}"
                        )
                        checks["order_line_consistency"] = False

        except Exception as e:
            checks["order_line_consistency"] = False
            logger.error(f"Order line consistency check failed: {e}")

        return checks
