# TPC-C Benchmark Tool - Refactored Architecture

## Overview

This is a refactored version of the TPC-C benchmark tool with a clean, maintainable architecture based on the design patterns from the `tpcc` module.

## Architecture

The codebase follows a layered architecture with clear separation of concerns:

### Directory Structure

```
src/
├── __init__.py           # Package initialization
├── main.py              # Main entry point
├── models/              # Data models (entities)
│   ├── __init__.py
│   ├── warehouse.py     # Warehouse entity
│   ├── district.py      # District entity
│   ├── customer.py      # Customer entity
│   ├── item.py          # Item entity
│   ├── stock.py         # Stock entity
│   ├── orders.py        # Orders entity
│   ├── order_line.py    # OrderLine entity
│   ├── new_order.py     # NewOrder entity
│   └── history.py       # History entity
├── database/            # Database layer
│   ├── __init__.py
│   ├── database_connection.py  # Connection management
│   └── schema_manager.py       # Schema operations
├── data_generator/      # Data generation
│   ├── __init__.py
│   └── tpcc_generator.py       # TPC-C data generator
├── executor/            # Business logic
│   ├── __init__.py
│   └── tpcc_executor.py        # Main execution logic
└── sql/                 # SQL files
    ├── create_tables.sql
    ├── create_index.sql
    └── ...
```

### Key Improvements

1. **Clean Architecture**: Each layer has a single responsibility
2. **Type Safety**: Uses Python type hints throughout
3. **Data Validation**: Models include validation logic
4. **Resource Management**: Proper connection handling with context managers
5. **Modularity**: Easy to extend and maintain
6. **Testing**: Designed for unit testing
7. **Logging**: Comprehensive logging for debugging

## Usage

### Command Line Interface

```bash
# Initialize database with scale factor 2
python -m tpcc.main --init --scale 2

# Run consistency checks
python -m tpcc.main --check

# Show database statistics
python -m tpcc.main --stats

# Enable verbose logging
python -m tpcc.main --init --scale 1 --verbose
```

### Programmatic Usage

```python
from tpcc.database.database_connection import DatabaseConnection
from tpcc.executor.tpcc_executor import TpccExecutor

# Initialize database
with DatabaseConnection("tpcc.db") as db:
    executor = TpccExecutor(db, scale_factor=1)
    executor.initialize_database()
    executor.load_data()

    # Run checks
    stats = executor.get_database_stats()
    print(stats)
```

## Migration from Old Architecture

The old architecture has been deprecated but remains for reference:
- `driver.py` - Replaced by `TpccExecutor`
- `generator.py` - Replaced by `TpccDataGenerator`
- `connection.py` - Replaced by `DatabaseConnection`

## Configuration

The tool uses sensible defaults but can be configured:

- **Database Path**: Specify via `--db-path` or programmatically
- **Scale Factor**: Number of warehouses (affects data size)
- **Logging**: Use `--verbose` for debug output

## Testing

The new architecture is designed for easy testing:

```python
# Unit test example
from tpcc.models import Warehouse
from tpcc.data_generator.tpcc_generator import RandomDataGenerator

# Test model validation
warehouse = Warehouse(
    w_id=1, w_name="Test", w_street_1="123 Main", w_street_2="",
    w_city="TestCity", w_state="CA", w_zip="12345", w_tax=0.1, w_ytd=1000.0
)

# Test data generation
generator = RandomDataGenerator(seed=42)
warehouse = next(generator.generate_warehouses())
```

## Performance Considerations

- **Batch Operations**: Data loading uses batch inserts
- **Connection Pooling**: Database connections are properly managed
- **Memory Usage**: Generators are used for large datasets
- **Indexing**: Proper indexes are created for query performance

## Future Enhancements

- [ ] Transaction support for ACID compliance
- [ ] Parallel data generation
- [ ] Performance metrics collection
- [ ] Configuration file support
- [ ] Web interface for monitoring
- [ ] Docker containerization