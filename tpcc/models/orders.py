"""
Orders model for TPC-C benchmark.
"""

from dataclasses import dataclass


@dataclass
class Orders:
    """Orders entity representing customer orders."""

    o_id: int
    o_c_id: int
    o_d_id: int
    o_w_id: int
    o_entry_d: str
    o_carrier_id: int
    o_ol_cnt: int
    o_all_local: int

    def __post_init__(self):
        """Validate orders data after initialization."""
        if not isinstance(self.o_id, int) or self.o_id <= 0:
            raise ValueError("Order ID must be a positive integer")
        if not isinstance(self.o_c_id, int) or not 1 <= self.o_c_id <= 3000:
            raise ValueError("Customer ID must be between 1 and 3000")
        if not isinstance(self.o_d_id, int) or not 1 <= self.o_d_id <= 10:
            raise ValueError("District ID must be between 1 and 10")
        if not isinstance(self.o_w_id, int) or self.o_w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
        if not isinstance(self.o_ol_cnt, int) or not 5 <= self.o_ol_cnt <= 15:
            raise ValueError("Order line count must be between 5 and 15")
        if not isinstance(self.o_carrier_id, int) or not 0 <= self.o_carrier_id <= 10:
            raise ValueError("Carrier ID must be between 0 and 10")
