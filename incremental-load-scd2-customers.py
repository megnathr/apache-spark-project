"""
Incremental load: customers -> dim_customer (SCD Type 2).

1. Timestamp CDC: read rows with last_updated > watermark
2. Dedupe: latest row per customer_id; skip re-signups whose email already belongs to a current customer
3. Classify against current dim rows: NEW or CHANGED (city/state/email/is_deleted); unchanged rows are ignored
4. Spark writes the new versions to a staging table
5. One Oracle transaction: close old versions, insert new versions, advance watermark

Safe to re-run: if it fails before the commit, the watermark has not moved and staging is overwritten.
"""

import os
from datetime import datetime

import oracledb
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DSN = os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")
USER = "sparklearn"
PASSWORD = "sparklearn123"
JDBC_URL = f"jdbc:oracle:thin:@{DSN}"
PROPS = {"user": USER, "password": PASSWORD, "driver": "oracle.jdbc.OracleDriver", "fetchsize": "10000"}
PIPELINE = "customers"
COLS = ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "CITY", "STATE", "SIGNUP_DATE", "IS_DELETED"]

spark = SparkSession.builder.appName("customers-incremental-scd2").getOrCreate()

# 1) Read watermark, then pull only newer rows
with oracledb.connect(user=USER, password=PASSWORD, dsn=DSN) as conn:
    cur = conn.cursor()
    cur.execute("SELECT last_watermark FROM etl_watermark WHERE pipeline_name = :p", p=PIPELINE)
    watermark = cur.fetchone()[0]

wm_literal = watermark.strftime("%Y-%m-%d %H:%M:%S.%f")

batch = spark.read.jdbc(
    JDBC_URL,
    f"(SELECT * FROM customers WHERE last_updated > TIMESTAMP '{wm_literal}') c",
    properties=PROPS,
).cache()

if batch.count() == 0:
    print(f"No changes after {watermark}")
    spark.stop()
    raise SystemExit

new_watermark = batch.agg(F.max("LAST_UPDATED")).first()[0]

# 2a) Dedupe within the batch: latest version per customer_id, normalized email
w_id = Window.partitionBy("CUSTOMER_ID").orderBy(F.col("LAST_UPDATED").desc())
batch = (
    batch.withColumn("EMAIL", F.lower(F.trim("EMAIL")))
    .withColumn("rn", F.row_number().over(w_id))
    .filter("rn = 1")
    .select(*COLS)
)

# 3) Compare against current dim rows
cur_dim = spark.read.jdbc(
    JDBC_URL,
    "(SELECT customer_id, city, state, email, is_deleted FROM dim_customer WHERE is_current = 1) d",
    properties=PROPS,
).select(
    F.col("CUSTOMER_ID").alias("D_ID"),
    F.col("CITY").alias("D_CITY"),
    F.col("STATE").alias("D_STATE"),
    F.col("EMAIL").alias("D_EMAIL"),
    F.col("IS_DELETED").alias("D_DELETED"),
)
j = batch.join(cur_dim, batch["CUSTOMER_ID"] == cur_dim["D_ID"], "left")

same = (
    F.col("CITY").eqNullSafe(F.col("D_CITY"))
    & F.col("STATE").eqNullSafe(F.col("D_STATE"))
    & F.col("EMAIL").eqNullSafe(F.col("D_EMAIL"))
    & F.col("IS_DELETED").eqNullSafe(F.col("D_DELETED"))
)

new_rows = j.filter(F.col("D_ID").isNull()).select(*COLS)

# the customer already exists in the dimension (D_ID not null) AND something about them is different.
changed = j.filter(F.col("D_ID").isNotNull() & ~same).select(*COLS)

# 2b) Dedupe re-signups: new customer_id but the email already belongs to a current customer -> skip
# semi = "is there a match? yes → keep"
dupes = new_rows.join(cur_dim, new_rows["EMAIL"] == cur_dim["D_EMAIL"], "left_semi")

# anti = "is there a match? no → keep"
new_clean = new_rows.join(cur_dim, new_rows["EMAIL"] == cur_dim["D_EMAIL"], "left_anti")

# ...and two new rows sharing an email inside this batch: keep the latest signup
w_em = Window.partitionBy("EMAIL").orderBy(F.col("SIGNUP_DATE").desc(), F.col("CUSTOMER_ID").desc())
new_clean = new_clean.withColumn("rn", F.row_number().over(w_em)).filter("rn = 1").select(*COLS)

# 4) Stage the new versions
stg = (
    new_clean.withColumn("ACTION", F.lit("NEW"))
    .unionByName(changed.withColumn("ACTION", F.lit("CHANGED")))
    .cache()
)
(
    stg.write.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "stg_customer_changes")
    .option("truncate", "true")
    .options(**{k: v for k, v in PROPS.items() if k != "fetchsize"})
    .mode("overwrite")
    .save()
)

n_new = stg.filter("ACTION = 'NEW'").count()
print(n_new)

n_changed = stg.filter("ACTION = 'CHANGED'").count()
print(n_changed)

n_dupes = dupes.count()
print(n_dupes)

# 5) Apply in ONE transaction: close old versions, insert new ones, advance watermark
run_ts = datetime.now()
print(run_ts)

with oracledb.connect(user=USER, password=PASSWORD, dsn=DSN) as conn:
    cur = conn.cursor()

    cur.setinputsizes(ts=oracledb.DB_TYPE_TIMESTAMP)
    cur.execute(
        """
        UPDATE dim_customer
        SET is_current = 0, effective_to = :ts
        WHERE is_current = 1
          AND customer_id IN (SELECT customer_id FROM stg_customer_changes WHERE action = 'CHANGED')
        """,
        ts=run_ts,
    )
    closed = cur.rowcount

    cur.setinputsizes(ts=oracledb.DB_TYPE_TIMESTAMP)
    cur.execute(
        """
        INSERT INTO dim_customer
            (customer_id, first_name, last_name, email, city, state, signup_date,
             is_deleted, effective_from, effective_to, is_current)
        SELECT customer_id, first_name, last_name, email, city, state, signup_date,
               is_deleted, :ts, NULL, 1
        FROM stg_customer_changes
        """,
        ts=run_ts,
    )
    inserted = cur.rowcount

    cur.setinputsizes(wm=oracledb.DB_TYPE_TIMESTAMP)
    cur.execute(
        "UPDATE etl_watermark SET last_watermark = :wm WHERE pipeline_name = :p",
        wm=new_watermark,
        p=PIPELINE,
    )
    conn.commit()

print(f"batch rows     : {batch.count():,}")
print(f"new customers  : {n_new}")
print(f"changed (SCD2) : {n_changed}")
print(f"dupes skipped  : {n_dupes}")
print(f"closed rows    : {closed}")
print(f"inserted rows  : {inserted}")
print(f"new watermark  : {new_watermark}")

spark.stop()