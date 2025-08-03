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
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    self._thread_local.db = DatabaseConnection(
                        host=self.db.host, port=self.db.port
                    )
                    self._thread_local.db.connect()
                    logger.debug(
                        f"Created database connection for thread {threading.current_thread().name}"
                    )
                    break
                except Exception as e:
                    retry_count += 1
                    logger.warning(
                        f"Failed to create database connection (attempt {retry_count}/{max_retries}): {e}"
                    )
                    if retry_count >= max_retries:
                        logger.error(
                            f"Failed to create database connection after {max_retries} attempts: {e}"
                        )
                        raise
                    time.sleep(0.1 * retry_count)  # Exponential backoff
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
        max_retries = 3
        retry_delay = 0.1  # 100ms
        max_execution_time = 60  # 60 second timeout per transaction

        for attempt in range(max_retries):
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

                if execution_time > max_execution_time:
                    logger.warning(
                        f"Transaction type {transaction_type} took {execution_time:.2f}s, exceeding timeout"
                    )

                if attempt > 0:
                    logger.debug(f"Transaction succeeded after {attempt + 1} attempts")

                return TransactionResult(
                    transaction_type=transaction_type,
                    success=success,
                    execution_time=execution_time,
                    timestamp=timestamp,
                    thread_id=thread_id,
                )

            except Exception as e:
                retry_count = attempt + 1
                execution_time = time.time() - start_time

                # Check for deadlock or timeout specific errors
                error_msg = str(e).lower()
                is_deadlock = (
                    "deadlock" in error_msg
                    or "timeout" in error_msg
                    or "lock" in error_msg
                )

                logger.warning(f"Transaction attempt {retry_count} failed: {e}")
                if is_deadlock:
                    logger.info(
                        "Detected potential deadlock, will retry with longer delay"
                    )

                if retry_count < max_retries:
                    # Exponential backoff with longer delay for deadlocks
                    delay = (
                        0.5 * retry_count if is_deadlock else retry_delay * retry_count
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Transaction failed after {max_retries} attempts: {e}"
                    )
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
                s_ytd = int(s_ytd)
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
        amount = self.data_generator.get_random_payment_amount()

        # Determine customer selection method (60% by ID, 40% by last name)
        if random.random() < 0.6:
            c_id = self.data_generator.get_random_customer_id()
            customer_query = c_id
            by_id = True
        else:
            c_last = self.data_generator.get_random_customer_last_name()
            customer_query = c_last
            by_id = False

        try:
            db.execute_update("BEGIN")

            # Step 1: Update warehouse YTD
            warehouse_result = db.execute_query(
                "SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd "
                "FROM warehouse WHERE w_id = ?",
                (w_id,),
            )

            if not warehouse_result:
                db.execute_update("ROLLBACK")
                return False

            w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd = (
                warehouse_result[0]
            )

            db.execute_update(
                "UPDATE warehouse SET w_ytd = w_ytd+? WHERE w_id = ?", (amount, w_id)
            )

            # Step 2: Update district YTD
            district_result = db.execute_query(
                "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd "
                "FROM district WHERE d_w_id = ? AND d_id = ?",
                (w_id, d_id),
            )

            if not district_result:
                db.execute_update("ROLLBACK")
                return False

            d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd = (
                district_result[0]
            )

            db.execute_update(
                "UPDATE district SET d_ytd = d_ytd+? WHERE d_w_id = ? AND d_id = ?",
                (amount, w_id, d_id),
            )

            # Step 3: Get customer information and update
            if by_id:
                customer_result = db.execute_query(
                    "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, "
                    "c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
                    "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data "
                    "FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                    (c_w_id, c_d_id, customer_query),
                )
            else:
                customer_result = db.execute_query(
                    "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, "
                    "c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
                    "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data "
                    "FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? "
                    "ORDER BY c_first",
                    (c_w_id, c_d_id, customer_query),
                )

            if not customer_result:
                db.execute_update("ROLLBACK")
                return False

            # Handle multiple customers with same last name (select middle one)
            if by_id:
                customer = customer_result[0]
            else:
                middle_idx = len(customer_result) // 2
                customer = customer_result[middle_idx]

            (
                c_id,
                c_first,
                c_middle,
                c_last,
                c_street_1,
                c_street_2,
                c_city,
                c_state,
                c_zip,
                c_phone,
                c_since,
                c_credit,
                c_credit_lim,
                c_discount,
                c_balance,
                c_ytd_payment,
                c_payment_cnt,
                c_data,
            ) = customer

            # Step 4: Update customer based on credit type
            c_id = int(c_id)
            new_balance = float(c_balance) - amount
            new_ytd_payment = float(c_ytd_payment) + amount
            new_payment_cnt = int(c_payment_cnt) + 1

            if c_credit == "BC":
                # Bad credit: update c_data with payment info
                payment_info = f"{c_id}{c_d_id}{c_w_id}{d_id}{w_id}{amount:.2f}"
                new_data = payment_info + str(c_data)
                new_data = new_data[:300]  # Truncate to 300 characters

                db.execute_update(
                    "UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ?, c_data = ? "
                    "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                    (
                        new_balance,
                        new_ytd_payment,
                        new_payment_cnt,
                        new_data,
                        c_w_id,
                        c_d_id,
                        c_id,
                    ),
                )
            else:
                # Good credit: simple update
                db.execute_update(
                    "UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? "
                    "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                    (
                        new_balance,
                        new_ytd_payment,
                        new_payment_cnt,
                        c_w_id,
                        c_d_id,
                        c_id,
                    ),
                )

            # Step 5: Insert history record
            h_data = w_name[:10] + "    " + d_name[:10]
            db.execute_update(
                "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    c_id,
                    c_d_id,
                    c_w_id,
                    d_id,
                    w_id,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    amount,
                    h_data,
                ),
            )

            db.execute_update("COMMIT")
            return True

        except Exception as e:
            logger.error(f"Payment transaction failed: {e}")
            db.execute_update("ROLLBACK")
            return False

    def _execute_delivery(self, db: DatabaseConnection) -> bool:
        """Execute Delivery transaction."""
        w_id = self.data_generator.get_random_warehouse_id()
        o_carrier_id = self.data_generator.get_random_carrier_id()

        try:
            db.execute_update("BEGIN")

            for d_id in range(1, 11):  # 10 districts per warehouse
                # Step 1: Find the oldest undelivered order (min o_id from new_orders)
                new_order_result = db.execute_query(
                    "SELECT MIN(no_o_id) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?",
                    (d_id, w_id),
                )

                if not new_order_result:
                    continue  # No undelivered orders for this district

                o_id = int(new_order_result[0][0])

                # Step 2: Remove the order from new_orders
                db.execute_update(
                    "DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?",
                    (o_id, d_id, w_id),
                )

                # Step 3: Update the order with carrier_id
                db.execute_update(
                    "UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?",
                    (o_carrier_id, o_id, d_id, w_id),
                )

                # Step 4: Update all order_lines with delivery date
                delivery_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                db.execute_update(
                    "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?",
                    (delivery_date, o_id, d_id, w_id),
                )

                # Step 5: Get customer ID and calculate order total
                order_result = db.execute_query(
                    "SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?",
                    (o_id, d_id, w_id),
                )
                if not order_result:
                    db.execute_update("ROLLBACK")
                    return False

                o_c_id = int(order_result[0][0])

                # Calculate total amount for this order
                total_result = db.execute_query(
                    "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?",
                    (o_id, d_id, w_id),
                )
                if not total_result:
                    db.execute_update("ROLLBACK")
                    return False

                order_total = float(total_result[0][0])

                # Step 6: Update customer balance and delivery count
                db.execute_update(
                    "UPDATE customer SET c_balance = c_balance+?, c_delivery_cnt = c_delivery_cnt+1 "
                    "WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?",
                    (order_total, o_c_id, d_id, w_id),
                )

            db.execute_update("COMMIT")
            return True

        except Exception as e:
            logger.error(f"Delivery transaction failed: {e}")
            db.execute_update("ROLLBACK")
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
        import signal

        if transaction_probabilities is None:
            # Default TPC-C mix: 45% NewOrder, 43% Payment, 4% each for others
            transaction_probabilities = [0.45, 0.43, 0.04, 0.04, 0.04]

        logger.info(f"Starting concurrent benchmark with {num_threads} threads")
        logger.info(f"Transactions per thread: {transactions_per_thread}")
        logger.info(f"Read-write ratio: {read_write_ratio}")
        logger.info("Press Ctrl+C to cancel benchmark and clean up resources")

        start_time = time.time()
        all_results = []

        def signal_handler(signum, frame):
            logger.info("Received interrupt signal, cancelling benchmark...")
            # Close all thread connections
            self.close_thread_connections()
            # Force exit the process immediately
            import os

            os._exit(0)

        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)

        # Use ThreadPoolExecutor for better thread management
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []

            # Submit tasks for each thread
            for thread_id in range(num_threads):
                # Submit all transactions for this thread as a batch
                future = executor.submit(
                    self._execute_thread_transactions,
                    thread_id,
                    transactions_per_thread,
                    read_write_ratio,
                    transaction_probabilities,
                )
                futures.append(future)

        # Collect results and flatten the nested list structure
        all_results = []
        for future in as_completed(futures):
            thread_results = future.result()
            all_results.extend(thread_results)

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
            if total_transactions > 0
            else 0
        )
        throughput_tps = (
            total_transactions / total_duration if total_duration > 0 else 0
        )

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

    def _execute_thread_transactions(
        self,
        thread_id: int,
        transaction_count: int,
        read_write_ratio: float,
        transaction_probabilities: List[float],
    ) -> List[TransactionResult]:
        """Execute a batch of transactions for a single thread."""
        results = []

        for _ in range(transaction_count):
            # Select transaction type based on read-write ratio
            if random.random() < read_write_ratio:
                # Read-write transactions (NewOrder, Payment, Delivery)
                txn_type = self._select_transaction_type(
                    transaction_probabilities[:3] + [0.0, 0.0]
                )
            else:
                # Read-only transactions (OrderStatus, StockLevel)
                txn_type = self._select_transaction_type(
                    [0.0, 0.0, 0.0] + transaction_probabilities[3:]
                )

            result = self._execute_transaction(txn_type, thread_id)
            results.append(result)

        return results

    def close_thread_connections(self):
        """Close all thread-local database connections."""
        if hasattr(self._thread_local, "db"):
            self._thread_local.db.close()
            del self._thread_local.db
