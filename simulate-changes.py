"""
Change simulator: mimics an app writing to the customers table.
Each run produces updates, soft deletes, new signups, and duplicate re-signups.
last_updated is set explicitly (app-managed updated_at, no trigger).

Usage:
    export ORACLE_USER=... ORACLE_PASS=... ORACLE_DSN=localhost:1521/FREEPDB1
    python simulate_changes.py --updates 300 --deletes 20 --inserts 100 --dupes 30
"""

import argparse
import os
import random

import oracledb
from faker import Faker

fake = Faker()

CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
    ("Seattle", "WA"), ("Denver", "CO"), ("Boston", "MA"), ("Atlanta", "GA"),
    ("Miami", "FL"), ("Austin", "TX"), ("Portland", "OR"), ("Phoenix", "AZ"),
]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "protonmail.com"]


def get_connection():
    return oracledb.connect(
        user="sparklearn",
        password="sparklearn123",
        dsn=os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1"),
    )


def new_email(first, last):
    return f"{first.lower()}.{last.lower()}{random.randint(1, 9999)}@{random.choice(DOMAINS)}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--updates", type=int, default=300)
    p.add_argument("--deletes", type=int, default=20)
    p.add_argument("--inserts", type=int, default=100)
    p.add_argument("--dupes", type=int, default=30)
    args = p.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    # Pick distinct live customers for updates and deletes in one go
    need = args.updates + args.deletes
    cur.execute(
        "SELECT customer_id, first_name, last_name FROM customers "
        "WHERE is_deleted = 0 ORDER BY DBMS_RANDOM.VALUE FETCH FIRST :n ROWS ONLY",
        n=need,
    )
    picked = cur.fetchall()
    to_update, to_delete = picked[: args.updates], picked[args.updates :]

    # 1) Updates: city/state change (SCD2 attribute), email changes ~30% of the time
    update_rows = []
    for cid, first, last in to_update:
        city, state = random.choice(CITIES)
        email = new_email(first, last) if random.random() < 0.3 else None
        update_rows.append({"city": city, "state": state, "email": email, "cid": cid})
    cur.executemany(
        "UPDATE customers SET city = :city, state = :state, "
        "email = COALESCE(:email, email), last_updated = SYSTIMESTAMP "
        "WHERE customer_id = :cid",
        update_rows,
    )

    # 2) Soft deletes
    cur.executemany(
        "UPDATE customers SET is_deleted = 1, last_updated = SYSTIMESTAMP "
        "WHERE customer_id = :1",
        [(cid,) for cid, _, _ in to_delete],
    )

    # 3) New signups
    insert_rows = []
    for _ in range(args.inserts):
        first, last = fake.first_name(), fake.last_name()
        city, state = random.choice(CITIES)
        insert_rows.append((first, last, new_email(first, last), city, state))
    cur.executemany(
        "INSERT INTO customers(first_name, last_name, email, city, state, signup_date) "
        "VALUES (:1, :2, :3, :4, :5, SYSDATE)",
        insert_rows,
    )

    # 4) Duplicates: same person re-signs up (same email, new customer_id)
    cur.execute(
        "SELECT first_name, last_name, email, city, state FROM customers "
        "WHERE is_deleted = 0 ORDER BY DBMS_RANDOM.VALUE FETCH FIRST :n ROWS ONLY",
        n=args.dupes,
    )
    dupe_rows = cur.fetchall()
    cur.executemany(
        "INSERT INTO customers(first_name, last_name, email, city, state, signup_date) "
        "VALUES (:1, :2, :3, :4, :5, SYSDATE)",
        dupe_rows,
    )

    conn.commit()
    print(
        f"updated={len(update_rows)} deleted={len(to_delete)} "
        f"inserted={len(insert_rows)} duplicated={len(dupe_rows)}"
    )

    cur.execute("SELECT COUNT(*), MAX(last_updated) FROM customers")
    total, max_ts = cur.fetchone()
    print(f"customers total={total:,} max(last_updated)={max_ts}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()