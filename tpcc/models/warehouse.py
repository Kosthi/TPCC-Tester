"""
Warehouse model for TPC-C benchmark.
"""

from dataclasses import dataclass


@dataclass
class Warehouse:
    """Warehouse entity representing a warehouse in the TPC-C benchmark."""

    w_id: int
    w_name: str
    w_street_1: str
    w_street_2: str
    w_city: str
    w_state: str
    w_zip: str
    w_tax: float
    w_ytd: float

    def __post_init__(self):
        """Validate warehouse data after initialization."""
        if not isinstance(self.w_id, int) or self.w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
        if not self.w_name or len(self.w_name) > 10:
            raise ValueError("Warehouse name must be 1-10 characters")
        if not isinstance(self.w_tax, (int, float)) or not 0 <= self.w_tax <= 0.2:
            raise ValueError("Warehouse tax must be between 0 and 0.2")
        if not isinstance(self.w_ytd, (int, float)) or self.w_ytd < 0:
            raise ValueError("Warehouse YTD must be non-negative")
