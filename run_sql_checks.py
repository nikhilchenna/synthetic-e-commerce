import sqlite3

DB = 'ecom.db'
conn = sqlite3.connect(DB)
cur = conn.cursor()

checks = []

# 1) Orders without matching customer
cur.execute("SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id=c.customer_id WHERE c.customer_id IS NULL")
missing_customers = cur.fetchone()[0]
checks.append(("orders_missing_customers", missing_customers))

# 2) Orders where sum(order_items.quantity*unit_price) != orders.total_amount
cur.execute('''
SELECT o.order_id, o.total_amount, IFNULL(SUM(oi.quantity*oi.unit_price),0) as items_total
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id
HAVING ROUND(items_total,2) != ROUND(o.total_amount,2)
''')
mismatch_rows = cur.fetchall()
checks.append(("orders_total_mismatches_count", len(mismatch_rows)))

# 3) Payments without orders
cur.execute("SELECT COUNT(*) FROM payments p LEFT JOIN orders o ON p.order_id=o.order_id WHERE o.order_id IS NULL")
missing_orders_in_payments = cur.fetchone()[0]
checks.append(("payments_missing_orders", missing_orders_in_payments))

# 4) Payments where sum of payments for order != orders.total_amount (per order)
cur.execute('''
SELECT o.order_id, o.total_amount, IFNULL(SUM(p.amount),0) as payments_total
FROM orders o
LEFT JOIN payments p ON o.order_id = p.order_id
GROUP BY o.order_id
HAVING ROUND(payments_total,2) != ROUND(o.total_amount,2)
''')
payment_mismatches = cur.fetchall()
checks.append(("payments_total_mismatches_count", len(payment_mismatches)))

# 5) Example combined join: orders with customer email, product names, and payment status (first payment)
cur.execute('''
SELECT o.order_id, o.order_date, o.total_amount, o.status, c.email, GROUP_CONCAT(DISTINCT p.name) as products, MIN(pay.payment_status) as payment_status
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.product_id
LEFT JOIN payments pay ON o.order_id = pay.order_id
GROUP BY o.order_id
ORDER BY o.order_id
''')
combined = cur.fetchall()

conn.close()

print("CHECKS:")
for k,v in checks:
    print(f"- {k}: {v}")

print("\nSAMPLE COMBINED JOIN (orders -> products -> payments -> customer):")
for row in combined:
    print(row)
