import oracledb
import faker
import numpy as np
from datetime import datetime, timedelta
import random
import time

# ── Config ──────────────────────────────────────────────────────────
ORACLE_USER = "sparklearn"
ORACLE_PASS = "sparklearn123"
ORACLE_DSN = "localhost:1521/FREEPDB1"

NUM_PRODUCTS = 500
NUM_CUSTOMERS = 200_000
NUM_ORDERS = 2_000_000
NUM_ORDER_ITEMS = 6_000_000
BATCH_SIZE = 10_000

fake = faker.Faker()
random.seed(42)
np.random.seed(42)
faker.Faker.seed(42)

# ── Product Catalog (realistic) ────────────────────────────────────
PRODUCT_CATALOG = {
    "Electronics": [
        ("MacBook Pro 16\"", 2499.99), ("MacBook Air M3", 1299.99),
        ("Dell XPS 15", 1799.99), ("ThinkPad X1 Carbon", 1649.99),
        ("iPad Pro 12.9\"", 1099.99), ("iPad Air", 599.99),
        ("Samsung Galaxy S24 Ultra", 1199.99), ("iPhone 15 Pro Max", 1199.99),
        ("iPhone 15", 799.99), ("Google Pixel 8 Pro", 999.99),
        ("Sony WH-1000XM5", 349.99), ("AirPods Pro 2", 249.99),
        ("Bose QC Ultra", 429.99), ("Samsung Galaxy Buds3", 179.99),
        ("LG OLED C4 65\"", 1799.99), ("Samsung QN90C 55\"", 1299.99),
        ("Sony A95L 55\"", 2499.99), ("Dell U2723QE Monitor", 619.99),
        ("Apple Watch Ultra 2", 799.99), ("Samsung Galaxy Watch 6", 329.99),
        ("Logitech MX Master 3S", 99.99), ("Apple Magic Keyboard", 199.99),
        ("Razer BlackWidow V4", 169.99), ("Logitech C920 Webcam", 69.99),
        ("Elgato Stream Deck", 149.99), ("Anker USB-C Hub", 35.99),
        ("CalDigit TS4 Dock", 399.99), ("Samsung T7 SSD 2TB", 159.99),
        ("WD Black SN850X 2TB", 149.99), ("Synology DS923+", 549.99),
    ],
    "Clothing": [
        ("Levi's 501 Original Jeans", 69.50), ("Nike Air Force 1", 110.00),
        ("Adidas Ultraboost 23", 190.00), ("Patagonia Down Jacket", 279.00),
        ("North Face Thermoball", 220.00), ("Columbia Rain Jacket", 89.99),
        ("Hanes Cotton T-Shirt 3pk", 19.99), ("Champion Hoodie", 45.00),
        ("Under Armour Shorts", 34.99), ("New Balance 574", 89.99),
        ("Dr. Martens 1460 Boots", 169.99), ("Birkenstock Arizona", 109.99),
        ("Ray-Ban Aviator", 163.00), ("Oakley Holbrook", 177.00),
        ("Carhartt Beanie", 19.99), ("Nike Dri-FIT Polo", 55.00),
        ("Brooks Ghost 15", 139.95), ("ASICS Gel-Kayano 30", 159.95),
        ("Lululemon Align Pants", 98.00), ("Uniqlo Heattech Thermal", 19.90),
    ],
    "Home & Kitchen": [
        ("Instant Pot Duo 8qt", 89.99), ("KitchenAid Stand Mixer", 379.99),
        ("Vitamix A3500 Blender", 549.99), ("Ninja Air Fryer XL", 119.99),
        ("Dyson V15 Detect", 749.99), ("iRobot Roomba j7+", 599.99),
        ("Keurig K-Elite", 169.99), ("Breville Barista Express", 699.99),
        ("Le Creuset Dutch Oven", 379.99), ("All-Clad D5 Cookware Set", 699.99),
        ("Nespresso Vertuo Next", 159.99), ("Cuisinart Food Processor", 199.99),
        ("Dyson Purifier HP07", 569.99), ("Coway Air Purifier", 229.99),
        ("Casper Original Mattress Queen", 1095.00), ("Purple Pillow", 159.00),
        ("Brooklinen Luxe Sheet Set", 168.00), ("Pottery Barn Throw", 79.50),
        ("Yankee Candle Large Jar", 30.99), ("simplehuman Trash Can", 79.99),
    ],
    "Books & Media": [
        ("Atomic Habits - James Clear", 16.99), ("Thinking Fast and Slow", 17.99),
        ("Designing Data-Intensive Apps", 44.99), ("Clean Code - R.Martin", 39.99),
        ("System Design Interview", 35.99), ("Spark: The Definitive Guide", 49.99),
        ("Learning Spark 2nd Ed", 49.99), ("Python Crash Course", 35.99),
        ("Kindle Paperwhite", 139.99), ("Audible Annual Membership", 149.50),
        ("Adobe Creative Cloud 1yr", 599.99), ("Microsoft 365 Family", 99.99),
        ("Coursera Plus Annual", 399.00), ("MasterClass Annual", 120.00),
        ("The Pragmatic Programmer", 49.99), ("Data Engineering with Python", 39.99),
        ("Kafka: The Definitive Guide", 49.99), ("Database Internals", 44.99),
        ("Fundamentals of DE", 44.99), ("DDIA Study Guide", 29.99),
    ],
    "Sports & Outdoors": [
        ("YETI Rambler 36oz", 44.99), ("Hydro Flask 32oz", 44.95),
        ("Coleman 6-Person Tent", 149.99), ("REI Co-op Backpack 40L", 139.00),
        ("Garmin Forerunner 265", 449.99), ("Fitbit Charge 6", 159.95),
        ("TRX Suspension Trainer", 179.95), ("Bowflex SelectTech 552", 429.00),
        ("Peloton Bike", 1445.00), ("NordicTrack Treadmill", 1499.00),
        ("Manduka PRO Yoga Mat", 120.00), ("Theragun Elite", 399.00),
        ("Osprey Atmos AG 65", 290.00), ("Black Diamond Headlamp", 29.95),
        ("Patagonia Black Hole Duffel", 169.00), ("RTIC Cooler 45qt", 229.99),
        ("Traeger Ironwood 650", 1299.99), ("Weber Spirit Grill", 489.00),
        ("YETI Tundra 45 Cooler", 325.00), ("Kelty Low Loveseat Camp Chair", 99.95),
    ],
    "Health & Beauty": [
        ("Oral-B iO Series 9", 299.99), ("Philips Sonicare 9900", 249.99),
        ("Waterpik Aquarius", 69.99), ("CeraVe Moisturizer 16oz", 17.99),
        ("La Roche-Posay Sunscreen", 33.99), ("Neutrogena Retinol Cream", 28.99),
        ("Olaplex No.3 Treatment", 30.00), ("Dyson Airwrap", 599.99),
        ("Revlon One-Step Dryer", 34.99), ("Braun Series 9 Pro", 299.99),
        ("Therabreath Mouthwash 2pk", 18.49), ("Vitamins D3+K2 365ct", 24.97),
        ("Athletic Greens AG1", 79.00), ("Whey Protein 5lb", 58.99),
        ("Creatine Monohydrate 1kg", 29.99), ("Fish Oil Omega-3 360ct", 23.97),
        ("Melatonin 5mg 250ct", 11.99), ("First Aid Kit 300pcs", 29.99),
        ("Brita Filter Pitcher", 27.99), ("Contigo Water Bottle 24oz", 14.99),
    ],
}

# US cities with state and approximate population weights
US_CITIES = [
    ("New York", "NY", 0.08), ("Los Angeles", "CA", 0.06), ("Chicago", "IL", 0.05),
    ("Houston", "TX", 0.04), ("Phoenix", "AZ", 0.03), ("Philadelphia", "PA", 0.03),
    ("San Antonio", "TX", 0.02), ("San Diego", "CA", 0.025), ("Dallas", "TX", 0.03),
    ("San Jose", "CA", 0.02), ("Austin", "TX", 0.025), ("Jacksonville", "FL", 0.015),
    ("Fort Worth", "TX", 0.015), ("Columbus", "OH", 0.015), ("Charlotte", "NC", 0.015),
    ("Indianapolis", "IN", 0.015), ("San Francisco", "CA", 0.02), ("Seattle", "WA", 0.025),
    ("Denver", "CO", 0.02), ("Nashville", "TN", 0.015), ("Portland", "OR", 0.015),
    ("Las Vegas", "NV", 0.015), ("Memphis", "TN", 0.01), ("Louisville", "KY", 0.01),
    ("Baltimore", "MD", 0.01), ("Milwaukee", "WI", 0.01), ("Albuquerque", "NM", 0.008),
    ("Tucson", "AZ", 0.008), ("Fresno", "CA", 0.008), ("Sacramento", "CA", 0.01),
    ("Atlanta", "GA", 0.025), ("Miami", "FL", 0.025), ("Raleigh", "NC", 0.012),
    ("Omaha", "NE", 0.008), ("Minneapolis", "MN", 0.015), ("Tampa", "FL", 0.015),
    ("New Orleans", "LA", 0.008), ("Cleveland", "OH", 0.008), ("Pittsburgh", "PA", 0.01),
    ("Cincinnati", "OH", 0.008), ("St. Louis", "MO", 0.01), ("Kansas City", "MO", 0.01),
    ("Salt Lake City", "UT", 0.01), ("Detroit", "MI", 0.01), ("Boston", "MA", 0.025),
    ("Orlando", "FL", 0.015), ("Richmond", "VA", 0.008), ("Boise", "ID", 0.006),
    ("Honolulu", "HI", 0.005), ("Anchorage", "AK", 0.003),
]

ORDER_STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED",
                  "SHIPPED", "SHIPPED", "PROCESSING", "PENDING", "CANCELLED", "REFUNDED"]

# Seasonal weights: more orders in Nov-Dec (holidays), less in Jan-Feb
MONTH_WEIGHTS = [0.06, 0.05, 0.07, 0.08, 0.08, 0.09, 0.09, 0.09, 0.08, 0.09, 0.11, 0.11]


def get_connection():
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)


def elapsed(start):
    return f"{time.time() - start:.1f}s"


def load_products(cursor):
    print("\n── Loading Products ──")
    start = time.time()
    rows = []
    for category, items in PRODUCT_CATALOG.items():
        for name, price in items:
            rows.append((name, category, price))

    cursor.executemany(
        "INSERT INTO products(name, category, price, created_date) VALUES (:1, :2, :3, SYSDATE - DBMS_RANDOM.VALUE(30,365))",
        rows
    )
    print(f"  Inserted {len(rows)} products ({elapsed(start)})")
    return len(rows)


def load_customers(cursor):
    print(f"\n── Loading {NUM_CUSTOMERS:,} Customers ──")
    start = time.time()
    cities, states, weights = zip(*US_CITIES)
    weights = np.array(weights)
    weights = weights / weights.sum()

    batch = []
    inserted = 0
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
               "protonmail.com", "aol.com", "live.com", "mail.com", "fastmail.com"]

    for i in range(NUM_CUSTOMERS):
        first = fake.first_name()
        last = fake.last_name()
        domain = random.choice(domains)
        email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{domain}"
        city_idx = np.random.choice(len(cities), p=weights)
        signup_days_ago = random.randint(1, 1095)  # up to 3 years

        batch.append((first, last, email, cities[city_idx], states[city_idx], signup_days_ago))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(
                "INSERT INTO customers(first_name, last_name, email, city, state, signup_date) "
                "VALUES (:1, :2, :3, :4, :5, SYSDATE - :6)",
                batch
            )
            inserted += len(batch)
            batch = []
            print(f"  {inserted:>10,} / {NUM_CUSTOMERS:,} ({elapsed(start)})")

    if batch:
        cursor.executemany(
            "INSERT INTO customers(first_name, last_name, email, city, state, signup_date) "
            "VALUES (:1, :2, :3, :4, :5, SYSDATE - :6)",
            batch
        )
        inserted += len(batch)

    print(f"  Done: {inserted:,} customers ({elapsed(start)})")


def load_orders(cursor, num_products):
    print(f"\n── Loading {NUM_ORDERS:,} Orders ──")
    start = time.time()

    # Pre-generate order dates with seasonal distribution
    base_date = datetime.now()
    order_dates = []
    for _ in range(NUM_ORDERS):
        days_ago = random.randint(1, 730)
        d = base_date - timedelta(days=days_ago)
        # Apply seasonal weight — bias toward heavier months
        month_w = MONTH_WEIGHTS[d.month - 1]
        if random.random() > month_w * 10:
            # Re-roll toward holiday season
            days_ago = random.randint(1, 730)
            d = base_date - timedelta(days=days_ago)
        order_dates.append(d)

    batch = []
    inserted = 0

    for i in range(NUM_ORDERS):
        customer_id = random.randint(1, NUM_CUSTOMERS)
        order_date = order_dates[i]
        status = random.choice(ORDER_STATUSES)

        # Realistic order totals — lognormal distribution (most orders $20-200, some large)
        total = round(float(np.random.lognormal(mean=3.8, sigma=1.0)), 2)
        total = min(total, 9999.99)  # cap

        batch.append((customer_id, order_date, status, total))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(
                "INSERT INTO orders(customer_id, order_date, status, total_amount) "
                "VALUES (:1, :2, :3, :4)",
                batch
            )
            inserted += len(batch)
            batch = []
            print(f"  {inserted:>10,} / {NUM_ORDERS:,} ({elapsed(start)})")

    if batch:
        cursor.executemany(
            "INSERT INTO orders(customer_id, order_date, status, total_amount) "
            "VALUES (:1, :2, :3, :4)",
            batch
        )
        inserted += len(batch)

    print(f"  Done: {inserted:,} orders ({elapsed(start)})")


def load_order_items(cursor, num_products):
    print(f"\n── Loading {NUM_ORDER_ITEMS:,} Order Items ──")
    start = time.time()

    # Product popularity follows power law (some products sell much more)
    product_weights = np.random.pareto(a=1.5, size=num_products) + 1
    product_weights = product_weights / product_weights.sum()

    # Pre-fetch product prices
    cursor.execute("SELECT product_id, price FROM products ORDER BY product_id")
    product_prices = {row[0]: float(row[1]) for row in cursor.fetchall()}
    product_ids = list(product_prices.keys())

    batch = []
    inserted = 0

    for i in range(NUM_ORDER_ITEMS):
        order_id = random.randint(1, NUM_ORDERS)
        prod_idx = np.random.choice(num_products, p=product_weights)
        product_id = product_ids[prod_idx]
        quantity = int(np.random.choice([1,1,1,1,2,2,2,3,3,4,5]))  # most buy 1-2
        unit_price = product_prices[product_id]

        # 25% chance of discount (5-30%)
        discount = round(random.uniform(5, 30), 2) if random.random() < 0.25 else 0

        batch.append((order_id, product_id, quantity, unit_price, discount))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(
                "INSERT INTO order_items(order_id, product_id, quantity, unit_price, discount) "
                "VALUES (:1, :2, :3, :4, :5)",
                batch
            )
            inserted += len(batch)
            batch = []
            print(f"  {inserted:>10,} / {NUM_ORDER_ITEMS:,} ({elapsed(start)})")

    if batch:
        cursor.executemany(
            "INSERT INTO order_items(order_id, product_id, quantity, unit_price, discount) "
            "VALUES (:1, :2, :3, :4, :5)",
            batch
        )
        inserted += len(batch)

    print(f"  Done: {inserted:,} order items ({elapsed(start)})")


def verify(cursor):
    print("\n── Verification ──")
    for tbl in ["products", "customers", "orders", "order_items"]:
        cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"  {tbl:<15} {cursor.fetchone()[0]:>12,} rows")

    print("\n── Sample Stats ──")
    cursor.execute("""
        SELECT category, COUNT(*), ROUND(AVG(price),2), MIN(price), MAX(price)
        FROM products GROUP BY category ORDER BY COUNT(*) DESC
    """)
    print(f"  {'Category':<20} {'Count':>6} {'AvgPrice':>10} {'Min':>10} {'Max':>10}")
    for row in cursor.fetchall():
        print(f"  {row[0]:<20} {row[1]:>6} {row[2]:>10.2f} {row[3]:>10.2f} {row[4]:>10.2f}")

    cursor.execute("""
        SELECT status, COUNT(*), ROUND(AVG(total_amount),2)
        FROM orders GROUP BY status ORDER BY COUNT(*) DESC
    """)
    print(f"\n  {'Status':<15} {'Orders':>10} {'AvgTotal':>10}")
    for row in cursor.fetchall():
        print(f"  {row[0]:<15} {row[1]:>10,} {row[2]:>10.2f}")

    cursor.execute("""
        SELECT c.state, COUNT(DISTINCT o.order_id) AS orders
        FROM customers c JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.state ORDER BY orders DESC FETCH FIRST 10 ROWS ONLY
    """)
    print(f"\n  {'State':<8} {'Orders':>10}")
    for row in cursor.fetchall():
        print(f"  {row[0]:<8} {row[1]:>10,}")


def main():
    print("=" * 60)
    print("  Spark Learning - Oracle Data Loader")
    print(f"  Target: {NUM_CUSTOMERS:,} customers | {NUM_ORDERS:,} orders | {NUM_ORDER_ITEMS:,} items")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    # Disable constraints temporarily for faster loading
    # 'R' - means referential constraint. i.e. a foreign key.
    print("\nDisabling foreign key constraints for bulk load...")
    cursor.execute("""
        BEGIN
            FOR c IN (SELECT constraint_name, table_name FROM user_constraints WHERE constraint_type = 'R') LOOP
                EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name || ' DISABLE CONSTRAINT ' || c.constraint_name;
            END LOOP;
        END;
    """)

    try:
        num_products = load_products(cursor)
        conn.commit()

        load_customers(cursor)
        conn.commit()

        load_orders(cursor, num_products)
        conn.commit()

        load_order_items(cursor, num_products)
        conn.commit()

        # Re-enable constraints
        print("\nRe-enabling foreign key constraints...")
        cursor.execute("""
            BEGIN
                FOR c IN (SELECT constraint_name, table_name FROM user_constraints WHERE constraint_type = 'R') LOOP
                    EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name || ' ENABLE NOVALIDATE CONSTRAINT ' || c.constraint_name;
                END LOOP;
            END;
        """)
        conn.commit()

        # Gather stats for optimizer
        print("Gathering table statistics...")
        for tbl in ["PRODUCTS", "CUSTOMERS", "ORDERS", "ORDER_ITEMS"]:
            cursor.execute(f"BEGIN DBMS_STATS.GATHER_TABLE_STATS('{ORACLE_USER.upper()}', '{tbl}'); END;")
        conn.commit()

        verify(cursor)

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    print("\n" + "=" * 60)
    print("  Data loading complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()