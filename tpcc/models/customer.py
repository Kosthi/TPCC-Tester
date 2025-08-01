"""
Customer model for TPC-C benchmark.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Customer:
    """Customer entity representing a customer within a district."""

    c_id: int
    c_d_id: int
    c_w_id: int
    c_first: str
    c_middle: str
    c_last: str
    c_street_1: str
    c_street_2: str
    c_city: str
    c_state: str
    c_zip: str
    c_phone: str
    c_since: str
    c_credit: str
    c_credit_lim: float
    c_discount: float
    c_balance: float
    c_ytd_payment: float
    c_payment_cnt: int
    c_delivery_cnt: int
    c_data: str

    def __post_init__(self):
        """Validate customer data after initialization."""
        if not isinstance(self.c_id, int) or not 1 <= self.c_id <= 3000:
            raise ValueError("Customer ID must be between 1 and 3000")
        if not isinstance(self.c_d_id, int) or not 1 <= self.c_d_id <= 10:
            raise ValueError("District ID must be between 1 and 10")
        if not isinstance(self.c_w_id, int) or self.c_w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
        if self.c_credit not in ["GC", "BC"]:
            raise ValueError("Credit must be either 'GC' or 'BC'")
        if (
            not isinstance(self.c_discount, (int, float))
            or not 0 <= self.c_discount <= 0.5
        ):
            raise ValueError("Discount must be between 0 and 0.5")
