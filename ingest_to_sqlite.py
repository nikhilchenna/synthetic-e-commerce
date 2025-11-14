#!/usr/bin/env python3

import sys
from pathlib import Path
import sqlite3
import pandas as pd

CSV_FILES = {
    "customers": "customers.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "payments": "payments.csv",
}

DB_PATH = "ecom.db"

def validate():
    any_problem = False
    print("\nChecking CSV files:\n")
    for k, fn in CSV_FILES.items():
        p = Path(fn)
        if not p.exists():
            print(f"[MISSING] {fn}")
            any_problem = True
        elif p.stat().st_size == 0:
            print(f"[EMPTY]   {fn}")
            any_problem = True
        else:
            print(f"[OK]      {fn} ({p.stat().st_size} bytes)")
    if any_problem:
        print("\nFix the above issues and run again.")
        sys.exit(1)

def load_csvs():
    dfs = {}
    for k, fn in CSV_FILES.items():
        try:
            df = pd.read_csv(fn)
        except Exception as e:
            print(f"\nError reading {fn}: {e}")
            sys.exit(1)
        dfs[k] = df
        print(f"Loaded {fn}: {df.shape[0]} rows, {df.shape[1]} columns")
    return dfs

def ingest(dfs):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    
    dfs["customers"].to_sql("customers", conn, if_exists="replace", index=False)
    dfs["products"].to_sql("products", conn, if_exists="replace", index=False)
    dfs["orders"].to_sql("orders", conn, if_exists="replace", index=False)
    dfs["order_items"].to_sql("order_items", conn, if_exists="replace", index=False)
    dfs["payments"].to_sql("payments", conn, if_exists="replace", index=False)

    # Indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);")

    conn.commit()
    conn.close()

def main():
    validate()
    dfs = load_csvs()
    ingest(dfs)
    print("\nData ingested into ecom.db successfully\n")

if __name__ == "__main__":
    main()
