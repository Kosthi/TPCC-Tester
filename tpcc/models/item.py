"""
Item model for TPC-C benchmark.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Item:
    """Item entity representing a catalog item."""

    i_id: int
    i_im_id: int
    i_name: str
    i_price: float
    i_data: str

    def __post_init__(self):
        """Validate item data after initialization."""
        if not isinstance(self.i_id, int) or not 1 <= self.i_id <= 100000:
            raise ValueError("Item ID must be between 1 and 100,000")
        if not isinstance(self.i_im_id, int) or self.i_im_id <= 0:
            raise ValueError("Image ID must be a positive integer")
        if not self.i_name or len(self.i_name) > 24:
            raise ValueError("Item name must be 1-24 characters")
        if not isinstance(self.i_price, (int, float)) or self.i_price < 0:
            raise ValueError("Item price must be non-negative")
