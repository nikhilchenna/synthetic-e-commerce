"""
Simple ingestion wrapper: read CSV files from a directory and load into SQLite using pandas.
Defaults to writing `ecom_synth.db` to stay consistent with existing check scripts.
"""
import sqlite3
from pathlib import Path
import pandas as pd

TABLES = ['customers','products','orders','order_items','payments']


def load_to_sqlite(csv_dir='data', db_file='ecom_synth.db'):
    csv_dir = Path(csv_dir)
    conn = sqlite3.connect(db_file)
    try:
        conn.execute('PRAGMA foreign_keys = ON;')
        for t in TABLES:
            p = csv_dir / f"{t}.csv"
            if not p.exists():
                raise FileNotFoundError(f"Missing CSV file: {p}")
            df = pd.read_csv(p)
            df.to_sql(t, conn, if_exists='replace', index=False)
            print(f"Wrote table {t}: {df.shape[0]} rows")

        # create indexes similar to existing ingestion script
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);")
        conn.commit()
    finally:
        conn.close()


if __name__ == '__main__':
    load_to_sqlite('data','ecom_synth.db')
