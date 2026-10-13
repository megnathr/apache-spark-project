import os
import oracledb
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DSN = "localhost:1521/FREEPDB1"
USER = "sparklearn"
PASSWORD = "sparklearn123"
JDBC_URL = f"jdbc:oracle:thin:@{DSN}"
PROPS = {
    "user": USER,
    "password": PASSWORD,
    "driver": "oracle.jdbc.OracleDriver",
    "fetchsize": "10000",
}
PIPELINE = "customers"

spark = SparkSession.builder.appName("customers-initial-load").getOrCreate()

# 1) Real id bounds (ids are not 1..N), so partitions are balanced
b = spark.read.jdbc(
    JDBC_URL,
    "(SELECT MIN(customer_id) AS mn, MAX(customer_id) AS mx FROM customers) b",
    properties=PROPS,
).first()
lo, hi = int(b["MN"]), int(b["MX"])

# 2) Partitioned read of the full source table
src = spark.read.jdbc(
    url=JDBC_URL,
    table="customers",
    column="customer_id",
    lowerBound=lo,
    upperBound=hi,
    numPartitions=4,
    properties=PROPS,
).cache()

# Watermark from the same snapshot we are about to load
watermark = src.agg(F.max("LAST_UPDATED")).first()[0]

# 3) Dedupe on normalized email: keep the latest signup (tie-break on highest id)
w = Window.partitionBy("EMAIL").orderBy(F.col("SIGNUP_DATE").desc(), F.col("CUSTOMER_ID").desc())
clean = (
    src.withColumn("EMAIL", F.lower(F.trim("EMAIL")))
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

# 4) SCD2 columns: everything starts as the current version
dim = (
    clean.select(
        "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL",
        "CITY", "STATE", "SIGNUP_DATE", "IS_DELETED",
    )
    .withColumn("EFFECTIVE_FROM", F.current_timestamp())
    .withColumn("EFFECTIVE_TO", F.lit(None).cast("timestamp"))
    .withColumn("IS_CURRENT", F.lit(1))
)

# 5) Write (truncate keeps the table DDL and the index)
(
    dim.write.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "dim_customer")
    .option("truncate", "true")
    .option("batchsize", "10000")
    .options(**{k: v for k, v in PROPS.items() if k != "fetchsize"})
    .mode("overwrite")
    .save()
)

# 6) Save the watermark for the incremental job
with oracledb.connect(user=USER, password=PASSWORD, dsn=DSN) as conn:
    cur = conn.cursor()
    # python-oracledb binds datetime as DATE by default, which drops fractional seconds
    cur.setinputsizes(wm=oracledb.DB_TYPE_TIMESTAMP)
    cur.execute(
        """
        MERGE INTO etl_watermark w
        USING (SELECT :p AS pipeline_name, :wm AS last_watermark FROM dual) s
        ON (w.pipeline_name = s.pipeline_name)
        WHEN MATCHED THEN UPDATE SET w.last_watermark = s.last_watermark
        WHEN NOT MATCHED THEN INSERT (pipeline_name, last_watermark)
             VALUES (s.pipeline_name, s.last_watermark)
        """,
        p=PIPELINE,
        wm=watermark,
    )
    conn.commit()

print(f"source rows : {src.count():,}")
print(f"dim rows    : {dim.count():,}")
print(f"watermark   : {watermark}")

spark.stop()