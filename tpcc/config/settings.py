from pathlib import Path

# Project Root
PROJECT_ROOT = Path(__file__).parent.parent

# Result Directory
RESULT_DIR = PROJECT_ROOT / "result"
RESULT_DIR.mkdir(exist_ok=True)

# Statistics File
STATISTICS_FILE = RESULT_DIR / "statistics_of_five_transactions.txt"

# New Orders File
NEW_ORDERS_FILE = RESULT_DIR / "timecost_and_num_of_NewOrders.txt"

# New Orders Image
NEW_ORDERS_IMAGE = RESULT_DIR / "timecost_and_num_of_NewOrders.jpg"

# Database File
DB_FILE = RESULT_DIR / "rds.db"

# RMDB server
HOST = "127.0.0.1"
PORT = 8765
