# Copilot Instructions for cursor-ecom-exercise

## Project Overview

This is a simple e-commerce data ingestion tool that loads CSV files into a SQLite database. The project has a single entry point (`ingest_to_sqlite.py`) with no external dependencies beyond pandas and sqlite3.

## Architecture

**Data Flow:**
1. CSV files (customers, products, orders, order_items, payments) → Read by pandas
2. DataFrames → Stored as SQLite tables (with `if_exists="replace"`)
3. Indexes created on foreign key relationships (order_id, customer_id, product_id)

**Key files:**
- `ingest_to_sqlite.py` - Single-file solution with three main phases: validation → loading → indexing

## Critical Patterns

**1. CSV Input Validation**
- `check_files_exist()` verifies all 5 required CSV files are present before processing
- Exit early with descriptive error messages if files are missing (defined in `CSV_FILES` dict)
- Always run this check before attempting to read CSV data

**2. DataFrame to SQLite Pattern**
- Use `DataFrame.to_sql(..., if_exists="replace", index=False)` for each table
- Indexes are created **after** table insertion via raw SQL (not through pandas)
- Enable foreign key constraints via `PRAGMA foreign_keys = ON;`

**3. Connection Management**
- SQLite connection opened in `ingest_into_sqlite()` with try/finally to ensure proper closure
- `conn.commit()` called once after all operations (batched writes)

**4. Database Schema**
Five tables created from CSVs with indexes on:
- `orders(customer_id)` - JOIN with customers
- `order_items(order_id)` - JOIN with orders
- `order_items(product_id)` - JOIN with products
- `payments(order_id)` - JOIN with orders

## Development Workflow

**Running the tool:**
```powershell
python ingest_to_sqlite.py
```
Requires: all 5 CSV files in current directory; outputs `ecom.db`

**No tests or build system** - this is a standalone ingestion script with manual validation via CSV file presence.

## Common Tasks

- **Adding a new CSV table:** Add entry to `CSV_FILES` dict, add `to_sql()` call in `ingest_into_sqlite()`, create indexes if needed
- **Modifying indexes:** Edit `index_statements` list in `ingest_into_sqlite()`
- **Changing database path:** Update `DB_PATH` constant (default: `ecom.db`)

