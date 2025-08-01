"""
NewOrder model for TPC-C benchmark.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class NewOrder:
    """NewOrder entity representing pending orders for delivery."""

    no_o_id: int
    no_d_id: int
    no_w_id: int

    def __post_init__(self):
        """Validate new order data after initialization."""
        if not isinstance(self.no_o_id, int) or self.no_o_id <= 0:
            raise ValueError("Order ID must be a positive integer")
        if not isinstance(self.no_d_id, int) or not 1 <= self.no_d_id <= 10:
            raise ValueError("District ID must be between 1 and 10")
        if not isinstance(self.no_w_id, int) or self.no_w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
