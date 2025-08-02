"""
Concurrent TPC-C benchmark executor with multi-threading support.
Provides multi-threaded read-write transaction execution capabilities.
"""

import logging
import threading
import time
import random

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

from tpcc.database.database_connection import DatabaseConnection
from tpcc.data_generator.tpcc_generator import TpccDataGenerator
from tpcc.models import *

logger = logging.getLogger(__name__)


@dataclass
class TransactionResult:
    """Result of a single transaction execution."""

    transaction_type: int
    success: bool
    execution_time: float
    timestamp: datetime
    thread_id: int


@dataclass
class BenchmarkResult:
    """Aggregated benchmark results."""

    total_transactions: int
    successful_transactions: int
    failed_transactions: int
    avg_response_time: float
    throughput_tps: float
    total_duration: float
    transaction_breakdown: Dict[int, int]
    per_thread_results: List[List[TransactionResult]]


class TransactionExecutor:
    """TPC-C transaction executor supporting multi-threaded read-write transactions."""

    # Transaction type constants
    NEW_ORDER = 0
    PAYMENT = 1
    DELIVERY = 2
    ORDER_STATUS = 3
    STOCK_LEVEL = 4

    def __init__(self, db_connection: DatabaseConnection, scale_factor: int = 1):
        """Initialize TPC-C transaction executor.

        Args:
            db_connection: Database connection instance
            scale_factor: Number of warehouses to test
        """
        self.db = db_connection
        self.scale_factor = scale_factor
        self.data_generator = TpccDataGenerator(scale_factor)

        # Thread-local storage for database connections
        self._thread_local = threading.local()

    def _get_thread_db_connection(self) -> DatabaseConnection:
        """Get thread-local database connection."""
        if not hasattr(self._thread_local, "db"):
            # Create new connection for this thread
            self._thread_local.db = DatabaseConnection(
                host=self.db.host, port=self.db.port
            )
            self._thread_local.db.connect()
        return self._thread_local.db

    def _execute_transaction(
        self, transaction_type: int, thread_id: int
    ) -> TransactionResult:
        """Execute a single transaction based on type.

        Args:
            transaction_type: Type of transaction to execute
            thread_id: ID of the executing thread

        Returns:
            TransactionResult with execution details
        """
        start_time = time.time()
        timestamp = datetime.now()

        try:
            db = self._get_thread_db_connection()

            if transaction_type == self.NEW_ORDER:
                success = self._execute_new_order(db)
            elif transaction_type == self.PAYMENT:
                success = self._execute_payment(db)
            elif transaction_type == self.DELIVERY:
                success = self._execute_delivery(db)
            elif transaction_type == self.ORDER_STATUS:
                success = self._execute_order_status(db)
            elif transaction_type == self.STOCK_LEVEL:
                success = self._execute_stock_level(db)
            else:
                raise ValueError(f"Unknown transaction type: {transaction_type}")

            execution_time = time.time() - start_time

            return TransactionResult(
                transaction_type=transaction_type,
                success=success,
                execution_time=execution_time,
                timestamp=timestamp,
                thread_id=thread_id,
            )

        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            execution_time = time.time() - start_time
            return TransactionResult(
                transaction_type=transaction_type,
                success=False,
                execution_time=execution_time,
                timestamp=timestamp,
                thread_id=thread_id,
            )

    def _execute_new_order(self, db: DatabaseConnection) -> bool:
        """Execute NewOrder transaction with complete TPC-C logic."""
        w_id = self.data_generator.get_random_warehouse_id()
        d_id = self.data_generator.get_random_district_id()
        c_id = self.data_generator.get_random_customer_id()

        # Generate order line items
        ol_cnt = self.data_generator.get_random_order_line_count()
        ol_i_id = [self.data_generator.get_random_item_id() for _ in range(ol_cnt)]
        ol_supply_w_id = [
            w_id
            if random.random() < 0.99
            else self.data_generator.get_random_warehouse_id()
            for _ in range(ol_cnt)
        ]
        ol_quantity = [self.data_generator.get_random_quantity() for _ in range(ol_cnt)]

        try:
            # Start transaction
            db.execute_update("BEGIN")

            # Phase 1: Get warehouse, district, and customer info
            # Get district info: tax and next order ID
            district_info = db.execute_query(
                "SELECT d_tax, d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?",
                (d_id, w_id),
            )
            if not district_info:
                db.execute_update("ROLLBACK")
                return False

            d_tax, d_next_o_id = district_info[0]
            d_tax = float(d_tax)
            o_id = int(d_next_o_id)

            # Update next order ID
            db.execute_update(
                "UPDATE district SET d_next_o_id = d_next_o_id+1 WHERE d_id = ? AND d_w_id = ?",
                (d_id, w_id),
            )

            # Get customer and warehouse info
            customer_info = db.execute_query(
                """SELECT c_discount, c_last, c_credit, w_tax
                   FROM customer,
                        warehouse
                   WHERE c_w_id = w_id
                     AND c_d_id = ?
                     AND c_id = ?
                     AND w_id = ?""",
                (d_id, c_id, w_id),
            )
            if not customer_info:
                db.execute_update("ROLLBACK")
                return False

            c_discount, c_last, c_credit, w_tax = customer_info[0]
            c_discount = float(c_discount)
            w_tax = float(w_tax)

            # Phase 2: Create order and new order
            o_entry_d = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            o_all_local = (
                1 if all(supply_w_id == w_id for supply_w_id in ol_supply_w_id) else 0
            )

            # Insert order
            db.execute_update(
                """INSERT INTO orders
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (o_id, d_id, w_id, c_id, o_entry_d, -1, ol_cnt, o_all_local),
            )

            # Insert new order
            db.execute_update(
                "INSERT INTO new_orders VALUES (?, ?, ?)",
                (o_id, d_id, w_id),
            )

            # Phase 3: Process order lines
            total_amount = 0
            for ol_number in range(1, ol_cnt + 1):
                # Get item info
                item_info = db.execute_query(
                    "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
                    (ol_i_id[ol_number - 1],),
                )
                if not item_info:
                    db.execute_update("ROLLBACK")
                    return False

                i_price, i_name, i_data = item_info[0]
                i_price = float(i_price)

                # Get stock info
                stock_info = db.execute_query(
                    f"SELECT s_quantity, s_dist_{d_id:02d}, s_ytd, s_order_cnt, s_remote_cnt, s_data "
                    "FROM stock WHERE s_i_id = ? AND s_w_id = ?",
                    (ol_i_id[ol_number - 1], ol_supply_w_id[ol_number - 1]),
                )
                if not stock_info:
                    db.execute_update("ROLLBACK")
                    return False

                s_quantity, s_dist, s_ytd, s_order_cnt, s_remote_cnt, s_data = (
                    stock_info[0]
                )
                s_quantity = int(s_quantity)
                s_ytd = float(s_ytd)
                s_order_cnt = int(s_order_cnt)
                s_remote_cnt = int(s_remote_cnt)

                # Update stock quantity
                if s_quantity >= ol_quantity[ol_number - 1] + 10:
                    s_quantity -= ol_quantity[ol_number - 1]
                else:
                    s_quantity = s_quantity - ol_quantity[ol_number - 1] + 91

                s_ytd += ol_quantity[ol_number - 1]
                s_order_cnt += 1
                if ol_supply_w_id[ol_number - 1] != w_id:
                    s_remote_cnt += 1

                # Update stock
                db.execute_update(
                    "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? "
                    "WHERE s_i_id = ? AND s_w_id = ?",
                    (
                        s_quantity,
                        s_ytd,
                        s_order_cnt,
                        s_remote_cnt,
                        ol_i_id[ol_number - 1],
                        ol_supply_w_id[ol_number - 1],
                    ),
                )

                # Calculate order line amount
                ol_amount = ol_quantity[ol_number - 1] * i_price
                brand_generic = (
                    "B" if "ORIGINAL" in i_data and "ORIGINAL" in s_data else "G"
                )

                # Insert order line
                db.execute_update(
                    """INSERT INTO order_line
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        o_id,
                        d_id,
                        w_id,
                        ol_number,
                        ol_i_id[ol_number - 1],
                        ol_supply_w_id[ol_number - 1],
                        "1970-01-01 00:00:00",
                        ol_quantity[ol_number - 1],
                        ol_amount,
                        s_dist,
                    ),
                )

                total_amount += ol_amount

            # Calculate final amount with discounts and taxes
            total_amount = total_amount * (1 - c_discount) * (1 + w_tax + d_tax)

            # Commit transaction
            db.execute_update("COMMIT")
            return True

        except Exception:
            db.execute_update("ROLLBACK")
            return False

    def _execute_payment(self, db: DatabaseConnection) -> bool:
        """Execute Payment transaction."""
        w_id = self.data_generator.get_random_warehouse_id()
        d_id = self.data_generator.get_random_district_id()
        c_w_id, c_d_id = self.data_generator.get_payment_customer_warehouse(w_id, d_id)

        try:
            query = """
                    UPDATE customer
                    SET c_balance     = c_balance - ?,
                        c_ytd_payment = c_ytd_payment + ?
                    WHERE c_w_id = ?
                      AND c_d_id = ?
                      AND c_id = ? \
                    """
            amount = self.data_generator.get_random_payment_amount()
            db.execute_update(
                query,
                (
                    amount,
                    amount,
                    c_w_id,
                    c_d_id,
                    self.data_generator.get_random_customer_id(),
                ),
            )
            return True
        except Exception:
            return False

    def _execute_delivery(self, db: DatabaseConnection) -> bool:
        """Execute Delivery transaction."""
        w_id = self.data_generator.get_random_warehouse_id()
        o_carrier_id = self.data_generator.get_random_carrier_id()

        try:
            # Simplified delivery transaction
            query = """
                    UPDATE orders
                    SET o_carrier_id = ?
                    WHERE o_w_id = ?
                      AND o_d_id = ?
                      AND
                        o_id IN (SELECT MIN(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id IS NULL) \
                    """
            for d_id in range(1, 11):  # 10 districts per warehouse
                db.execute_update(query, (o_carrier_id, w_id, d_id, w_id, d_id))
            return True
        except Exception:
            return False

    def _execute_order_status(self, db: DatabaseConnection) -> bool:
        """Execute OrderStatus transaction."""
        w_id = self.data_generator.get_random_warehouse_id()
        d_id = self.data_generator.get_random_district_id()
        c_id = self.data_generator.get_random_customer_id()

        try:
            query = """
                    SELECT *
                    FROM orders
                    WHERE o_w_id = ?
                      AND o_d_id = ?
                      AND o_c_id = ?
                    ORDER BY o_id DESC LIMIT 1 \
                    """
            result = db.execute_query(query, (w_id, d_id, c_id))
            return len(result) > 0
        except Exception:
            return False

    def _execute_stock_level(self, db: DatabaseConnection) -> bool:
        """Execute StockLevel transaction."""
        w_id = self.data_generator.get_random_warehouse_id()
        d_id = self.data_generator.get_random_district_id()
        threshold = self.data_generator.get_random_stock_threshold()

        try:
            query = """
                    SELECT COUNT(DISTINCT s_i_id)
                    FROM stock
                    WHERE s_w_id = ?
                      AND s_quantity < ? \
                    """
            result = db.execute_query(query, (w_id, threshold))
            return len(result) > 0
        except Exception:
            return False

    def _select_transaction_type(self, transaction_probabilities: List[float]) -> int:
        """Select transaction type based on probabilities."""
        import random

        return random.choices(
            range(len(transaction_probabilities)), weights=transaction_probabilities
        )[0]

    def run_concurrent_benchmark(
        self,
        num_threads: int,
        transactions_per_thread: int,
        read_write_ratio: float = 0.5,
        transaction_probabilities: Optional[List[float]] = None,
    ) -> BenchmarkResult:
        """Run concurrent benchmark with specified threads and transactions.

        Args:
            num_threads: Number of concurrent threads
            transactions_per_thread: Number of transactions per thread
            read_write_ratio: Ratio of read-write vs read-only transactions (0.0-1.0)
            transaction_probabilities: Probabilities for each transaction type

        Returns:
            BenchmarkResult with aggregated metrics
        """
        if transaction_probabilities is None:
            # Default TPC-C mix: 45% NewOrder, 43% Payment, 4% each for others
            transaction_probabilities = [0.45, 0.43, 0.04, 0.04, 0.04]

        logger.info(f"Starting concurrent benchmark with {num_threads} threads")
        logger.info(f"Transactions per thread: {transactions_per_thread}")
        logger.info(f"Read-write ratio: {read_write_ratio}")

        start_time = time.time()
        all_results = []

        # Use ThreadPoolExecutor for better thread management
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []

            # Submit tasks for each thread
            for thread_id in range(num_threads):
                for _ in range(transactions_per_thread):
                    # Select transaction type based on read-write ratio
                    if random.random() < read_write_ratio:
                        # Read-write transactions (NewOrder, Payment, Delivery)
                        txn_type = self._select_transaction_type(
                            [0.5, 0.0, 0.0, 0.0, 0.0]
                        )
                    else:
                        # Read-only transactions (OrderStatus, StockLevel)
                        txn_type = self._select_transaction_type(
                            [0.5, 0.0, 0.0, 0.0, 0.0]
                        )

                    future = executor.submit(
                        self._execute_transaction, txn_type, thread_id
                    )
                    futures.append(future)

            # Collect results
            for future in as_completed(futures):
                result = future.result()
                all_results.append(result)

        total_duration = time.time() - start_time

        # Aggregate results
        total_transactions = len(all_results)
        successful_transactions = sum(1 for r in all_results if r.success)
        failed_transactions = total_transactions - successful_transactions

        # Calculate per-transaction type breakdown
        transaction_breakdown = {}
        for txn_type in range(5):
            transaction_breakdown[txn_type] = sum(
                1 for r in all_results if r.transaction_type == txn_type
            )

        # Calculate performance metrics
        avg_response_time = (
            sum(r.execution_time for r in all_results) / total_transactions
        )
        throughput_tps = total_transactions / total_duration

        # Group results by thread
        per_thread_results = [[] for _ in range(num_threads)]
        for result in all_results:
            per_thread_results[result.thread_id].append(result)

        return BenchmarkResult(
            total_transactions=total_transactions,
            successful_transactions=successful_transactions,
            failed_transactions=failed_transactions,
            avg_response_time=avg_response_time,
            throughput_tps=throughput_tps,
            total_duration=total_duration,
            transaction_breakdown=transaction_breakdown,
            per_thread_results=per_thread_results,
        )

    def close_thread_connections(self):
        """Close all thread-local database connections."""
        if hasattr(self._thread_local, "db"):
            self._thread_local.db.close()
            del self._thread_local.db
