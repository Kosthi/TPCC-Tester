"""
TPC-C compliant data generator.
Refactored based on tpcc module's design for better maintainability.
"""

import logging
from typing import List, Dict, Any, Iterator, Tuple
from datetime import datetime, timedelta
import random
import string

from ..models import (
    Warehouse,
    District,
    Customer,
    Item,
    Stock,
    Orders,
    OrderLine,
    NewOrder,
    History,
)

logger = logging.getLogger(__name__)


class TpccDataGenerator:
    """Generates TPC-C compliant test data with proper scaling."""

    def __init__(self, scale_factor: int = 1):
        """Initialize data generator with scale factor.

        Args:
            scale_factor: Number of warehouses to generate (default: 1)
        """
        self.scale_factor = max(1, scale_factor)
        self.random = RandomDataGenerator()

        # TPC-C constants
        self.WAREHOUSES_PER_SCALE = 1
        self.DISTRICTS_PER_WAREHOUSE = 10
        self.CUSTOMERS_PER_DISTRICT = 3000
        self.ITEMS_TOTAL = 100000
        self.ORDERS_PER_DISTRICT = 3000
        self.NEW_ORDERS_PER_DISTRICT = 900

    def generate_warehouses(self) -> Iterator[Warehouse]:
        """Generate warehouse data."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            yield Warehouse(
                w_id=w_id,
                w_name=f"W{w_id:02d}",
                w_street_1=self.random.generate_street_address(),
                w_street_2=self.random.generate_street_address(),
                w_city=self.random.generate_city(),
                w_state=self.random.generate_state(),
                w_zip=self.random.generate_zip(),
                w_tax=self.random.generate_tax(),
                w_ytd=300000.0,
            )

    def generate_districts(self) -> Iterator[District]:
        """Generate district data for all warehouses."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                yield District(
                    d_id=d_id,
                    d_w_id=w_id,
                    d_name=f"D{d_id:02d}",
                    d_street_1=self.random.generate_street_address(),
                    d_street_2=self.random.generate_street_address(),
                    d_city=self.random.generate_city(),
                    d_state=self.random.generate_state(),
                    d_zip=self.random.generate_zip(),
                    d_tax=self.random.generate_tax(),
                    d_ytd=30000.0,
                    d_next_o_id=3001,
                )

    def generate_customers(self) -> Iterator[Customer]:
        """Generate customer data for all districts."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                for c_id in range(1, self.CUSTOMERS_PER_DISTRICT + 1):
                    credit = "GC" if self.random.random_int(0, 100) < 90 else "BC"

                    yield Customer(
                        c_id=c_id,
                        c_d_id=d_id,
                        c_w_id=w_id,
                        c_first=self.random.generate_first_name(),
                        c_middle="OE",
                        c_last=self.random.generate_last_name(c_id),
                        c_street_1=self.random.generate_street_address(),
                        c_street_2=self.random.generate_street_address(),
                        c_city=self.random.generate_city(),
                        c_state=self.random.generate_state(),
                        c_zip=self.random.generate_zip(),
                        c_phone=self.random.generate_phone(),
                        c_since=self.random.generate_timestamp(),
                        c_credit=credit,
                        c_credit_lim=50000.0,
                        c_discount=self.random.generate_discount(),
                        c_balance=-10.0,
                        c_ytd_payment=10.0,
                        c_payment_cnt=1,
                        c_delivery_cnt=0,
                        c_data=self.random.generate_data(50, 300),
                    )

    def generate_items(self) -> Iterator[Item]:
        """Generate item catalog."""
        for i_id in range(1, self.ITEMS_TOTAL + 1):
            data = self.random.generate_data(26, 50)
            if self.random.random_int(0, 100) < 10:
                pos = self.random.random_int(0, len(data) - 8)
                data = data[:pos] + "ORIGINAL" + data[pos + 8 :]

            yield Item(
                i_id=i_id,
                i_im_id=self.random.random_int(1, 10000),
                i_name=self.random.generate_item_name(),
                i_price=self.random.generate_price(),
                i_data=data,
            )

    def generate_stock(self) -> Iterator[Stock]:
        """Generate stock data for all items in all warehouses."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            for i_id in range(1, self.ITEMS_TOTAL + 1):
                data = self.random.generate_data(26, 50)
                if self.random.random_int(0, 100) < 10:
                    pos = self.random.random_int(0, len(data) - 8)
                    data = data[:pos] + "ORIGINAL" + data[pos + 8 :]

                yield Stock(
                    s_i_id=i_id,
                    s_w_id=w_id,
                    s_quantity=self.random.random_int(10, 100),
                    s_dist_01=self.random.generate_dist_info(),
                    s_dist_02=self.random.generate_dist_info(),
                    s_dist_03=self.random.generate_dist_info(),
                    s_dist_04=self.random.generate_dist_info(),
                    s_dist_05=self.random.generate_dist_info(),
                    s_dist_06=self.random.generate_dist_info(),
                    s_dist_07=self.random.generate_dist_info(),
                    s_dist_08=self.random.generate_dist_info(),
                    s_dist_09=self.random.generate_dist_info(),
                    s_dist_10=self.random.generate_dist_info(),
                    s_ytd=0,
                    s_order_cnt=0,
                    s_remote_cnt=0,
                    s_data=data,
                )

    def generate_orders(self) -> Iterator[Orders]:
        """Generate order data."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                for o_id in range(1, self.ORDERS_PER_DISTRICT + 1):
                    carrier_id = 0 if o_id > 2100 else self.random.random_int(1, 10)

                    yield Orders(
                        o_id=o_id,
                        o_c_id=self.random.random_int(1, 3000),
                        o_d_id=d_id,
                        o_w_id=w_id,
                        o_entry_d=self.random.generate_timestamp(),
                        o_carrier_id=carrier_id,
                        o_ol_cnt=10,
                        o_all_local=1,
                    )

    def generate_new_orders(self) -> Iterator[NewOrder]:
        """Generate new order data."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                for no_id in range(2101, 2101 + self.NEW_ORDERS_PER_DISTRICT):
                    yield NewOrder(no_o_id=no_id, no_d_id=d_id, no_w_id=w_id)

    def generate_history(self) -> Iterator[History]:
        """Generate history data for all customers."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                for c_id in range(1, self.CUSTOMERS_PER_DISTRICT + 1):
                    yield History(
                        h_c_id=c_id,
                        h_c_d_id=d_id,
                        h_c_w_id=w_id,
                        h_d_id=d_id,
                        h_w_id=w_id,
                        h_date=self.random.generate_timestamp(),
                        h_amount=10.0,  # Initial payment
                        h_data="Initial deposit",
                    )

    def generate_order_lines(self) -> Iterator[OrderLine]:
        """Generate order line data for all orders."""
        warehouse_count = self.WAREHOUSES_PER_SCALE * self.scale_factor

        for w_id in range(1, warehouse_count + 1):
            for d_id in range(1, self.DISTRICTS_PER_WAREHOUSE + 1):
                for o_id in range(1, self.ORDERS_PER_DISTRICT + 1):
                    ol_cnt = 10

                    for ol_number in range(1, ol_cnt + 1):
                        i_id = self.random.random_int(1, self.ITEMS_TOTAL)
                        quantity = self.random.random_int(1, 10)
                        amount = quantity * self.random.generate_price()

                        # For orders > 2100, delivery info is populated
                        delivery_d = "1970-01-01 00:00:00"  # default value
                        if o_id <= 2100:
                            delivery_d = self.random.generate_timestamp()

                        yield OrderLine(
                            ol_o_id=o_id,
                            ol_d_id=d_id,
                            ol_w_id=w_id,
                            ol_number=ol_number,
                            ol_i_id=i_id,
                            ol_supply_w_id=w_id,
                            ol_delivery_d=delivery_d,
                            ol_quantity=quantity,
                            ol_amount=amount,
                            ol_dist_info=self.random.generate_dist_info(),
                        )

    def generate_all_data(self) -> Dict[str, Iterator[Any]]:
        """Generate all TPC-C data types as iterators."""
        return {
            "warehouses": self.generate_warehouses(),
            "districts": self.generate_districts(),
            "customers": self.generate_customers(),
            "items": self.generate_items(),
            "stock": self.generate_stock(),
            "orders": self.generate_orders(),
            "new_orders": self.generate_new_orders(),
            "history": self.generate_history(),
            "order_lines": self.generate_order_lines(),
        }

    # Methods for concurrent transaction data generation
    def get_random_warehouse_id(self) -> int:
        """Get random warehouse ID."""
        return self.random.random_int(1, self.scale_factor)

    def get_random_district_id(self) -> int:
        """Get random district ID (1-10)."""
        return self.random.random_int(1, self.DISTRICTS_PER_WAREHOUSE)

    def get_random_customer_id(self) -> int:
        """Get random customer ID (1-3000)."""
        return self.random.random_int(1, self.CUSTOMERS_PER_DISTRICT)

    def get_random_item_id(self) -> int:
        """Get random item ID (1-100000)."""
        return self.random.random_int(1, self.ITEMS_TOTAL)

    def get_random_order_line_count(self) -> int:
        """Get random number of order lines (5-15)."""
        return self.random.random_int(5, 15)

    def get_random_quantity(self) -> int:
        """Get random quantity (1-10)."""
        return self.random.random_int(1, 10)

    def get_payment_customer_warehouse(self, w_id: int, d_id: int) -> tuple[int, int]:
        """Get customer warehouse and district for payment transaction."""
        # 85% chance customer is in same warehouse
        if self.random.random_int(1, 100) <= 85:
            c_w_id = w_id
            # 15% chance customer is in different district
            if self.random.random_int(1, 100) <= 15:
                c_d_id = self.random.random_int(1, self.DISTRICTS_PER_WAREHOUSE)
            else:
                c_d_id = d_id
        else:
            # Customer in different warehouse
            c_w_id = self.get_random_warehouse_id()
            while c_w_id == w_id:  # Ensure different warehouse
                c_w_id = self.get_random_warehouse_id()
            c_d_id = self.get_random_district_id()

        return c_w_id, c_d_id

    def get_random_payment_amount(self) -> float:
        """Get random payment amount (1.00-5000.00)."""
        return self.random.random_float(1.0, 5000.0)

    def get_random_carrier_id(self) -> int:
        """Get random carrier ID (1-10)."""
        return self.random.random_int(1, 10)

    def get_random_stock_threshold(self) -> int:
        """Get random stock level threshold (10-20)."""
        return self.random.random_int(10, 20)


class RandomDataGenerator:
    """Utility class for generating TPC-C compliant random data."""

    def __init__(self, seed: int = None):
        """Initialize random generator with optional seed for reproducibility."""
        if seed is not None:
            random.seed(seed)

    def random_int(self, min_val: int, max_val: int) -> int:
        """Generate random integer in range [min_val, max_val]."""
        return random.randint(min_val, max_val)

    def random_float(self, min_val: float, max_val: float) -> float:
        """Generate random float in range [min_val, max_val]."""
        return random.uniform(min_val, max_val)

    def generate_string(
        self, length: int, charset: str = string.ascii_letters + string.digits
    ) -> str:
        """Generate random string of given length."""
        return "".join(random.choice(charset) for _ in range(length))

    def generate_street_address(self) -> str:
        """Generate realistic street address."""
        return f"{self.random_int(1, 9999)} {self.generate_string(5, string.ascii_uppercase)} St"

    def generate_city(self) -> str:
        """Generate city name."""
        cities = [
            "Springfield",
            "Rivertown",
            "Oakland",
            "Madison",
            "Lincoln",
            "Franklin",
        ]
        return random.choice(cities)

    def generate_state(self) -> str:
        """Generate state code."""
        states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
        return random.choice(states)

    def generate_zip(self) -> str:
        """Generate ZIP code."""
        return f"{self.random_int(10000, 99999)}{self.random_int(1000, 9999)}"

    def generate_phone(self) -> str:
        """Generate phone number."""
        return f"({self.random_int(100, 999)}) {self.random_int(100, 999)}-{self.random_int(1000, 9999)}"

    def generate_tax(self) -> float:
        """Generate tax rate (0-20%)."""
        return round(self.random_float(0, 2000) / 10000, 4)

    def generate_discount(self) -> float:
        """Generate discount rate (0-50%)."""
        return round(self.random_float(0, 5000) / 10000, 4)

    def generate_price(self) -> float:
        """Generate item price ($1.00-$100.00)."""
        return round(self.random_float(100, 10000) / 100, 2)

    def generate_item_name(self) -> str:
        """Generate item name."""
        prefixes = ["Red", "Blue", "Green", "Large", "Small", "Premium", "Standard"]
        items = ["Widget", "Gadget", "Tool", "Device", "Product", "Item"]
        return f"{random.choice(prefixes)} {random.choice(items)}"

    def generate_data(self, min_len: int, max_len: int) -> str:
        """Generate random data string within length range."""
        length = self.random_int(min_len, max_len)
        return self.generate_string(length)

    def generate_dist_info(self) -> str:
        """Generate district information."""
        return self.generate_string(24, string.ascii_uppercase + string.digits)

    def generate_last_name(self, customer_id: int) -> str:
        """Generate last name based on customer ID (TPC-C specific)."""
        syllables = [
            "BAR",
            "OUGHT",
            "ABLE",
            "PRI",
            "PRES",
            "ESE",
            "ANTI",
            "CALLY",
            "ATION",
            "EING",
        ]

        if customer_id < 1000:
            last_name = syllables[customer_id // 100]
        else:
            last_name = (
                syllables[customer_id % 1000 // 100] + syllables[customer_id // 1000]
            )

        return last_name

    def generate_first_name(self) -> str:
        """Generate first name."""
        names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona"]
        return random.choice(names)

    def generate_timestamp(self) -> str:
        """Generate random timestamp within last 2 years in format 'YYYY-MM-DD HH:MM:SS'."""
        days_ago = self.random_int(0, 730)
        timestamp = datetime.now() - timedelta(days=days_ago)
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")
