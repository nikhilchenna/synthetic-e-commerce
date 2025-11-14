import sqlite3
db = "ecom.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

tables = ["customers","products","orders","order_items","payments"]
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(t, "rows ->", cur.fetchone()[0])

# Example query: show first 5 orders joined with customer email
cur.execute("""
SELECT o.order_id, o.order_date, o.total_amount, o.status, c.email
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
ORDER BY o.order_id
LIMIT 5
""")
for row in cur.fetchall():
    print(row)

conn.close()
