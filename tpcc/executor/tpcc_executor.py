"""
TPC-C benchmark executor.
Refactored to provide clean separation of concerns and better maintainability.
"""

import logging
from typing import Dict, Any

from ..database.database_connection import DatabaseConnection
from ..database.schema_manager import SchemaManager
from ..data_generator.tpcc_generator import TpccDataGenerator
from ..models import *
from .load_executor import LoadExecutor
from .consistency_checker import ConsistencyCheckExecutor
from .transaction_executor import TransactionExecutor, BenchmarkResult

logger = logging.getLogger(__name__)


class TpccExecutor:
    """Main executor for TPC-C benchmark operations."""

    def __init__(self, db_connection: DatabaseConnection, scale_factor: int = 1):
        """Initialize TPC-C executor.

        Args:
            db_connection: Database connection instance
            scale_factor: Number of warehouses to test
        """
        self.db = db_connection
        self.scale_factor = scale_factor
        self.schema_manager = SchemaManager(db_connection)
        self.data_generator = TpccDataGenerator(scale_factor)
        self.load_executor = LoadExecutor(db_connection)
        self.consistency_checker = ConsistencyCheckExecutor(db_connection, scale_factor)
        self.transaction_executor = TransactionExecutor(db_connection, scale_factor)

    def initialize_database(self) -> None:
        """Initialize database with schema and data."""
        logger.info("Initializing TPC-C database...")

        # Create schema
        self.schema_manager.create_schema()

        # Create indexes
        self.schema_manager.create_indexes()

        # We not check Validate schema now
        # if not self.schema_manager.validate_schema():
        #     raise RuntimeError("Schema validation failed")

        logger.info("Database initialized successfully")

    def load_data(self) -> None:
        """Load TPC-C data into database."""
        logger.info("Loading TPC-C data...")

        data_generators = self.data_generator.generate_all_data()
        self.load_executor.load_all_data(data_generators)

        logger.info("TPC-C data loaded successfully")

    def run_consistency_checks(self) -> Dict[str, bool]:
        """Run consistency checks on loaded data."""
        return self.consistency_checker.run_consistency_checks()

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        return self.consistency_checker.get_database_stats()

    def run_benchmark(
        self,
        num_threads: int = 1,
        transactions_per_thread: int = 100,
        read_write_ratio: float = 0.5,
        duration_seconds: int = 0,
        **kwargs,
    ) -> BenchmarkResult:
        """Run TPC-C benchmark with transaction execution.

        Args:
            num_threads: Number of concurrent threads
            transactions_per_thread: Number of transactions per thread
            read_write_ratio: Ratio of read-write vs read-only transactions
            duration_seconds: Duration of benchmark in seconds
            **kwargs: Additional arguments for transaction executor

        Returns:
            BenchmarkResult with performance metrics
        """
        logger.info(f"Starting TPC-C benchmark with {num_threads} threads")

        # Calculate transactions based on duration if provided
        if duration_seconds > 0:
            # Estimate transactions based on TPS and duration
            estimated_tps = 50  # Conservative estimate
            total_transactions = estimated_tps * duration_seconds
            transactions_per_thread = max(1, total_transactions // num_threads)

        return self.transaction_executor.run_concurrent_benchmark(
            num_threads=num_threads,
            transactions_per_thread=transactions_per_thread,
            read_write_ratio=read_write_ratio,
            **kwargs,
        )

    def run_performance_test(
        self, duration_seconds: int = 60, thread_counts: list = None, **kwargs
    ) -> Dict[int, BenchmarkResult]:
        """Run performance test with varying thread counts.

        Args:
            duration_seconds: Duration for each test
            thread_counts: List of thread counts to test
            **kwargs: Additional arguments for transaction executor

        Returns:
            Dictionary mapping thread count to benchmark results
        """
        if thread_counts is None:
            thread_counts = [1, 2, 4, 8, 16]

        results = {}

        for threads in thread_counts:
            logger.info(f"Running test with {threads} threads")
            result = self.run_benchmark(
                num_threads=threads, duration_seconds=duration_seconds, **kwargs
            )
            results[threads] = result

        return results
