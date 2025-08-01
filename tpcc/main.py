"""
TPC-C Benchmark Main Entry Point.
Refactored to use the new clean architecture with concurrent execution support.
"""

import argparse
import logging
import sys

from tpcc.database.database_connection import DatabaseConnection
from tpcc.executor.tpcc_executor import TpccExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging level based on verbosity."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


def main():
    """Main entry point for TPC-C benchmark."""
    parser = argparse.ArgumentParser(description="TPC-C Benchmark Tool")
    parser.add_argument(
        "--scale", "-s", type=int, default=1, help="Scale factor (number of warehouses)"
    )
    parser.add_argument(
        "--host", type=str, default="localhost", help="RMDB server host"
    )
    parser.add_argument("--port", type=int, default=8765, help="RMDB server port")
    parser.add_argument(
        "--init", action="store_true", help="Initialize database with schema and data"
    )
    parser.add_argument("--check", action="store_true", help="Run consistency checks")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    # Concurrent benchmark arguments
    parser.add_argument(
        "--benchmark", action="store_true", help="Run concurrent benchmark"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of concurrent threads for benchmark",
    )
    parser.add_argument(
        "--transactions", type=int, default=100, help="Transactions per thread"
    )
    parser.add_argument(
        "--rw-ratio", type=float, default=0.5, help="Read-write ratio (0.0-1.0)"
    )
    parser.add_argument(
        "--txn-probs",
        type=float,
        nargs=5,
        default=[0.45, 0.43, 0.04, 0.04, 0.04],
        metavar=("NEWORDER", "PAYMENT", "DELIVERY", "ORDERSTATUS", "STOCKLEVEL"),
        help="Transaction type probabilities",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    # Validate scale factor
    if args.scale < 1:
        logger.error("Scale factor must be at least 1")
        sys.exit(1)

    try:
        # Initialize database connection
        with DatabaseConnection(args.host, args.port) as db:
            executor = TpccExecutor(db, args.scale)

            if args.init:
                logger.info(
                    f"Initializing TPC-C benchmark with scale factor {args.scale}"
                )
                executor.initialize_database()
                executor.load_data()

            if args.check:
                checks = executor.run_consistency_checks()
                if not all(checks.values()):
                    logger.error("Some consistency checks failed")
                    sys.exit(1)
                else:
                    logger.info("All consistency checks passed")

            if args.stats:
                stats = executor.get_database_stats()
                logger.info("Database Statistics:")
                for table, count in stats.items():
                    logger.info(f"  {table}: {count:,}")

            if args.benchmark:
                logger.info("Starting TPC-C benchmark...")

                result = executor.run_benchmark(
                    num_threads=args.threads,
                    transactions_per_thread=args.transactions,
                    read_write_ratio=args.rw_ratio,
                    transaction_probabilities=args.txn_probs,
                )

                # Print results
                print("\n" + "=" * 60)
                print("TPC-C CONCURRENT BENCHMARK RESULTS")
                print("=" * 60)
                print(
                    f"Configuration: {args.threads} threads, {args.transactions} transactions/thread"
                )
                print(f"Scale Factor: {args.scale} warehouses")
                print(f"Read-Write Ratio: {args.rw_ratio}")

                print(f"\nPerformance:")
                print(f"  Total Transactions: {result.total_transactions:,}")
                print(f"  Successful: {result.successful_transactions:,}")
                print(f"  Failed: {result.failed_transactions:,}")
                print(
                    f"  Success Rate: {(result.successful_transactions / result.total_transactions) * 100:.2f}%"
                )
                print(f"  Total Duration: {result.total_duration:.2f} seconds")
                print(
                    f"  Average Response Time: {result.avg_response_time * 1000:.2f} ms"
                )
                print(f"  Throughput: {result.throughput_tps:.2f} TPS")

                print(f"\nTransaction Mix:")
                txn_names = [
                    "NewOrder",
                    "Payment",
                    "Delivery",
                    "OrderStatus",
                    "StockLevel",
                ]
                for txn_type, count in result.transaction_breakdown.items():
                    if count > 0:
                        percentage = (count / result.total_transactions) * 100
                        print(f"  {txn_names[txn_type]}: {count:,} ({percentage:.1f}%)")

                print("=" * 60)

            logger.info(
                "Use --init to initialize database, --check for consistency checks, "
                "--benchmark for concurrent benchmark, or --stats for statistics"
            )

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
