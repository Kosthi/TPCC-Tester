"""
Stock model for TPC-C benchmark.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Stock:
    """Stock entity representing inventory for an item in a warehouse."""

    s_i_id: int
    s_w_id: int
    s_quantity: int
    s_dist_01: str
    s_dist_02: str
    s_dist_03: str
    s_dist_04: str
    s_dist_05: str
    s_dist_06: str
    s_dist_07: str
    s_dist_08: str
    s_dist_09: str
    s_dist_10: str
    s_ytd: int
    s_order_cnt: int
    s_remote_cnt: int
    s_data: str

    def __post_init__(self):
        """Validate stock data after initialization."""
        if not isinstance(self.s_i_id, int) or self.s_i_id <= 0:
            raise ValueError("Item ID must be a positive integer")
        if not isinstance(self.s_w_id, int) or self.s_w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
        if not isinstance(self.s_quantity, int) or not 0 <= self.s_quantity <= 100:
            raise ValueError("Stock quantity must be between 0 and 100")
        if not isinstance(self.s_ytd, int) or self.s_ytd < 0:
            raise ValueError("YTD must be non-negative")
        if not isinstance(self.s_order_cnt, int) or self.s_order_cnt < 0:
            raise ValueError("Order count must be non-negative")
        if not isinstance(self.s_remote_cnt, int) or self.s_remote_cnt < 0:
            raise ValueError("Remote count must be non-negative")
