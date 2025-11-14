"""Create DataFrames from synthetic CSVs and call ingest_into_sqlite to write `ecom_synth.db`.
This imports the functions from `ingest_to_sqlite.py` and uses the same ingestion routine.
"""
import pandas as pd
import ingest_to_sqlite

# Read synthetic CSVs into same keys expected by ingest()
dfs = {
    "customers": pd.read_csv("synthetic_customers.csv"),
    "products": pd.read_csv("synthetic_products.csv"),
    "orders": pd.read_csv("synthetic_orders.csv"),
    "order_items": pd.read_csv("synthetic_order_items.csv"),
    "payments": pd.read_csv("synthetic_payments.csv"),
}

if __name__ == '__main__':
    # Use a separate DB path so we don't overwrite the main ecom.db
    ingest_to_sqlite.DB_PATH = "ecom_synth.db"
    ingest_to_sqlite.ingest(dfs)
    print("Synthetic data ingested into ecom_synth.db")
