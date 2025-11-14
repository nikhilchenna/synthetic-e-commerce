"""
Simple synthetic data generator for the e-commerce ingestion project.
Generates five CSVs: customers, products, orders, order_items, payments
ensuring referential integrity (orders.customer_id in customers, order_items.product_id in products).
"""
import csv
import random
from pathlib import Path
from datetime import date, timedelta

random.seed(1)

def _date_str(days_from=0):
    return (date.today() - timedelta(days=days_from)).isoformat()

def create_synthetic_data(output_dir='data', n_customers=10, n_products=10, n_orders=20):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # customers
    with open(out/'customers.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_id','first_name','last_name','email','signup_date','country'])
        for i in range(1, n_customers+1):
            writer.writerow([i, f'First{i}', f'Last{i}', f'user{i}@example.com', _date_str(30+i), random.choice(['US','GB','CA','DE','FR'])])

    # products
    with open(out/'products.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['product_id','sku','name','category','price'])
        for i in range(1, n_products+1):
            price = round(random.uniform(5.0, 200.0), 2)
            writer.writerow([i, f'SKU-{i:04d}', f'Product {i}', random.choice(['Electronics','Home','Books','Sports','Beauty']), price])

    # orders and order_items
    order_items = []
    with open(out/'orders.csv', 'w', newline='', encoding='utf-8') as f_orders, \
         open(out/'order_items.csv', 'w', newline='', encoding='utf-8') as f_items:
        ow = csv.writer(f_orders)
        iw = csv.writer(f_items)
        ow.writerow(['order_id','customer_id','order_date','total_amount','status'])
        iw.writerow(['order_item_id','order_id','product_id','quantity','unit_price'])

        order_item_id = 1
        for oid in range(1, n_orders+1):
            cust_id = random.randint(1, n_customers)
            num_items = random.randint(1, 3)
            items = []
            for _ in range(num_items):
                pid = random.randint(1, n_products)
                qty = random.randint(1,4)
                # read price approximately by regenerating same random but it's okay to sample new
                price = round(random.uniform(5.0, 200.0), 2)
                items.append((order_item_id, oid, pid, qty, price))
                iw.writerow([order_item_id, oid, pid, qty, price])
                order_item_id += 1

            total = round(sum(q*pr for (_,_,_,q,pr) in items), 2)
            ow.writerow([oid, cust_id, _date_str(60-oid), total, 'completed'])

    # payments (one payment per order matching total)
    with open(out/'payments.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['payment_id','order_id','payment_method','payment_date','amount','payment_status'])
        for pid, oid in enumerate(range(1, n_orders+1), start=1):
            # read order total from orders.csv would be heavier; recompute a plausible amount
            # For simplicity we read orders file to get exact total
            pass

    # read orders to pick totals for payments
    orders_totals = {}
    with open(out/'orders.csv', 'r', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            orders_totals[int(r['order_id'])] = r['total_amount']

    with open(out/'payments.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['payment_id','order_id','payment_method','payment_date','amount','payment_status'])
        for pid, oid in enumerate(range(1, n_orders+1), start=1):
            amt = float(orders_totals[oid])
            writer.writerow([pid, oid, random.choice(['card','paypal','bank_transfer']), _date_str(59-oid), amt, 'paid'])

    print(f"Synthetic CSV files written to: {out.resolve()}")


if __name__ == '__main__':
    create_synthetic_data(output_dir='data', n_customers=5, n_products=5, n_orders=5)
