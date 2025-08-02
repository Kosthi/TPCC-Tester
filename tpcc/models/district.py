"""
District model for TPC-C benchmark.
"""

from dataclasses import dataclass


@dataclass
class District:
    """District entity representing a district within a warehouse."""

    d_id: int
    d_w_id: int
    d_name: str
    d_street_1: str
    d_street_2: str
    d_city: str
    d_state: str
    d_zip: str
    d_tax: float
    d_ytd: float
    d_next_o_id: int

    def __post_init__(self):
        """Validate district data after initialization."""
        if not isinstance(self.d_id, int) or not 1 <= self.d_id <= 10:
            raise ValueError("District ID must be between 1 and 10")
        if not isinstance(self.d_w_id, int) or self.d_w_id <= 0:
            raise ValueError("Warehouse ID must be a positive integer")
        if not self.d_name or len(self.d_name) > 10:
            raise ValueError("District name must be 1-10 characters")
        if not isinstance(self.d_tax, (int, float)) or not 0 <= self.d_tax <= 0.2:
            raise ValueError("District tax must be between 0 and 0.2")
        if not isinstance(self.d_next_o_id, int) or self.d_next_o_id <= 0:
            raise ValueError("Next order ID must be a positive integer")
