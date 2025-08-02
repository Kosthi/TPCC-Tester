"""
History model for TPC-C benchmark.
"""

from dataclasses import dataclass


@dataclass
class History:
    """History entity representing transaction history for customers."""

    h_c_id: int
    h_c_d_id: int
    h_c_w_id: int
    h_d_id: int
    h_w_id: int
    h_date: str
    h_amount: float
    h_data: str

    def __post_init__(self):
        """Validate history data after initialization."""
        if not isinstance(self.h_c_id, int) or not 1 <= self.h_c_id <= 3000:
            raise ValueError("Customer ID must be between 1 and 3000")
        if not isinstance(self.h_c_d_id, int) or not 1 <= self.h_c_d_id <= 10:
            raise ValueError("Customer district ID must be between 1 and 10")
        if not isinstance(self.h_c_w_id, int) or self.h_c_w_id <= 0:
            raise ValueError("Customer warehouse ID must be a positive integer")
        if not isinstance(self.h_d_id, int) or not 1 <= self.h_d_id <= 10:
            raise ValueError("District ID must be between 1 and 10")
        if not isinstance(self.h_w_id, int) or self.h_w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
        if not isinstance(self.h_amount, (int, float)) or self.h_amount < 0:
            raise ValueError("Amount must be non-negative")
        if not self.h_data or len(self.h_data) > 24:
            raise ValueError("History data must be 1-24 characters")
