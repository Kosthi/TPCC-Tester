"""
OrderLine model for TPC-C benchmark.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class OrderLine:
    """OrderLine entity representing individual items within an order."""

    ol_o_id: int
    ol_d_id: int
    ol_w_id: int
    ol_number: int
    ol_i_id: int
    ol_supply_w_id: int
    ol_delivery_d: Optional[str]
    ol_quantity: int
    ol_amount: float
    ol_dist_info: str

    def __post_init__(self):
        """Validate order line data after initialization."""
        if not isinstance(self.ol_o_id, int) or self.ol_o_id <= 0:
            raise ValueError("Order ID must be a positive integer")
        if not isinstance(self.ol_d_id, int) or not 1 <= self.ol_d_id <= 10:
            raise ValueError("District ID must be between 1 and 10")
        if not isinstance(self.ol_w_id, int) or self.ol_w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
        if not isinstance(self.ol_number, int) or not 1 <= self.ol_number <= 15:
            raise ValueError("Order line number must be between 1 and 15")
        if not isinstance(self.ol_i_id, int) or self.ol_i_id <= 0:
            raise ValueError("Item ID must be a positive integer")
        if not isinstance(self.ol_supply_w_id, int) or self.ol_supply_w_id <= 0:
            raise ValueError("Supply warehouse ID must be a positive integer")
        if not isinstance(self.ol_quantity, int) or self.ol_quantity <= 0:
            raise ValueError("Quantity must be a positive integer")
        if not isinstance(self.ol_amount, (int, float)) or self.ol_amount < 0:
            raise ValueError("Amount must be non-negative")
