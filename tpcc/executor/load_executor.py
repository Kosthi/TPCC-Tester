"""
Data Load Executor for TPC-C benchmark.
Encapsulates all data loading operations with clean separation of concerns.
"""

import logging
from typing import Iterator

from ..database.database_connection import DatabaseConnection
from ..models import Warehouse, District, Item, Customer, Stock, Orders, NewOrder

logger = logging.getLogger(__name__)


class LoadExecutor:
    """Dedicated executor for loading TPC-C benchmark data."""

    def __init__(self, db_connection: DatabaseConnection):
        """Initialize Load Executor.

        Args:
            db_connection: Database connection instance
        """
        self.db = db_connection

    def load_warehouses(self, warehouses: Iterator[Warehouse]) -> None:
        """Load warehouse data."""
        logger.info("Loading warehouse data...")
        with self.db.get_cursor() as cursor:
            for warehouse in warehouses:
                cursor.execute(
                    """
                    INSERT INTO warehouse
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        warehouse.w_id,
                        warehouse.w_name,
                        warehouse.w_street_1,
                        warehouse.w_street_2,
                        warehouse.w_city,
                        warehouse.w_state,
                        warehouse.w_zip,
                        warehouse.w_tax,
                        warehouse.w_ytd,
                    ),
                )
        logger.info("Warehouse data loaded successfully")

    def load_districts(self, districts: Iterator[District]) -> None:
        """Load district data."""
        logger.info("Loading district data...")
        with self.db.get_cursor() as cursor:
            for district in districts:
                cursor.execute(
                    """
                    INSERT INTO district
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        district.d_id,
                        district.d_w_id,
                        district.d_name,
                        district.d_street_1,
                        district.d_street_2,
                        district.d_city,
                        district.d_state,
                        district.d_zip,
                        district.d_tax,
                        district.d_ytd,
                        district.d_next_o_id,
                    ),
                )
        logger.info("District data loaded successfully")

    def load_items(self, items: Iterator[Item]) -> None:
        """Load item data."""
        logger.info("Loading item data...")
        with self.db.get_cursor() as cursor:
            for item in items:
                cursor.execute(
                    """
                    INSERT INTO item
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (item.i_id, item.i_im_id, item.i_name, item.i_price, item.i_data),
                )
        logger.info("Item data loaded successfully")

    def load_customers(self, customers: Iterator[Customer]) -> None:
        """Load customer data."""
        logger.info("Loading customer data...")
        with self.db.get_cursor() as cursor:
            for customer in customers:
                cursor.execute(
                    """
                    INSERT INTO customer
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        customer.c_id,
                        customer.c_d_id,
                        customer.c_w_id,
                        customer.c_first,
                        customer.c_middle,
                        customer.c_last,
                        customer.c_street_1,
                        customer.c_street_2,
                        customer.c_city,
                        customer.c_state,
                        customer.c_zip,
                        customer.c_phone,
                        customer.c_since,
                        customer.c_credit,
                        customer.c_credit_lim,
                        customer.c_discount,
                        customer.c_balance,
                        customer.c_ytd_payment,
                        customer.c_payment_cnt,
                        customer.c_delivery_cnt,
                        customer.c_data,
                    ),
                )
        logger.info("Customer data loaded successfully")

    def load_stock(self, stocks: Iterator[Stock]) -> None:
        """Load stock data."""
        logger.info("Loading stock data...")
        with self.db.get_cursor() as cursor:
            for stock in stocks:
                cursor.execute(
                    """
                    INSERT INTO stock
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        stock.s_i_id,
                        stock.s_w_id,
                        stock.s_quantity,
                        stock.s_dist_01,
                        stock.s_dist_02,
                        stock.s_dist_03,
                        stock.s_dist_04,
                        stock.s_dist_05,
                        stock.s_dist_06,
                        stock.s_dist_07,
                        stock.s_dist_08,
                        stock.s_dist_09,
                        stock.s_dist_10,
                        stock.s_ytd,
                        stock.s_order_cnt,
                        stock.s_remote_cnt,
                        stock.s_data,
                    ),
                )
        logger.info("Stock data loaded successfully")

    def load_orders(self, orders: Iterator[Orders]) -> None:
        """Load orders data."""
        logger.info("Loading orders data...")
        with self.db.get_cursor() as cursor:
            for order in orders:
                cursor.execute(
                    """
                    INSERT INTO orders
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order.o_id,
                        order.o_c_id,
                        order.o_d_id,
                        order.o_w_id,
                        order.o_entry_d,
                        order.o_carrier_id,
                        order.o_ol_cnt,
                        order.o_all_local,
                    ),
                )
        logger.info("Orders data loaded successfully")

    def load_new_orders(self, new_orders: Iterator[NewOrder]) -> None:
        """Load new orders data."""
        logger.info("Loading new orders data...")
        with self.db.get_cursor() as cursor:
            for new_order in new_orders:
                cursor.execute(
                    """
                    INSERT INTO new_orders
                    VALUES (?, ?, ?)
                    """,
                    (new_order.no_o_id, new_order.no_d_id, new_order.no_w_id),
                )
        logger.info("New orders data loaded successfully")

    def load_all_data(self, data_generators: dict) -> None:
        """Load all TPC-C data in correct order.

        Args:
            data_generators: Dictionary containing data generators for each table
        """
        logger.info("Starting comprehensive data loading...")

        # Load data in logical order to respect foreign key constraints
        self.load_warehouses(data_generators["warehouses"])
        self.load_districts(data_generators["districts"])
        self.load_items(data_generators["items"])
        self.load_customers(data_generators["customers"])
        self.load_stock(data_generators["stock"])
        self.load_orders(data_generators["orders"])
        self.load_new_orders(data_generators["new_orders"])

        logger.info("All TPC-C data loaded successfully")
