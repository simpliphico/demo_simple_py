# ETL Project in PySpark

A data processing pipeline built with PySpark for Extract, Transform, and Load (ETL) operations.

## Project Structure

```
demo_simple_py/
├── src/
│   ├── etl/
│   │   ├── extract.py      # Data extraction and cleaning
│   │   ├── transform.py    # Data transformations
│   │   ├── exporter.py     # Data export functionality
│   │   └── utils.py        # Utility functions and logging
│   └── main.py             # Main application entry point
├── test/
│   └── test_extract.py     # Unit tests for extraction module
├── data/
│   ├── input/              # Input CSV files
│   └── output/             # Processed data output
├── config/
│   └── config.yml          # Configuration files
└── requirements.txt        # Python dependencies
```

## Features

- **Data Extraction**: Load and validate CSV files with automatic schema detection
- **Data Cleaning**: Remove duplicates, handle missing values, and clean column names
- **Data Validation**: Type checking and format validation for dates, integers, and doubles
- **Logging**: Comprehensive logging system for monitoring pipeline execution
- **Testing**: Unit tests with PySpark and pytest

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd demo_simple_py
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the project in development mode:
```bash
pip install -e .
```

## Usage

### Running the Main Application

```bash
python src/main.py
```

### Running Tests

```bash
python -m pytest test/ -v
```

### Code Quality Checks

```bash
ruff check src test
```

## Data Files

The project expects the following CSV files in the `data/input/` directory:

- `products_uuid.csv`: Product information (product_id, product_name, category)
- `store_uuid.csv`: Store information (store_id, store_name, location)
- `sales_uuid.csv`: Sales transactions (transaction_id, store_id, product_id, quantity, transaction_date, price)

## Configuration

The project uses a configuration file located at `config/config.yml` for pipeline settings.

## Development

### Adding New Features

1. Create new modules in the `src/etl/` directory
2. Add corresponding tests in the `test/` directory
3. Update the main application in `src/main.py`

### Code Style

The project uses:
- **Ruff** for linting and formatting
- **Black** compatible formatting (88 character line length)
- **Type hints** for better code documentation

## License

This project is licensed under the MIT License.
