# SCD Type 2 Customer Dimension with PySpark + Oracle

Incremental SCD Type 2 pipeline that tracks historical changes to customer records, built with PySpark on top of an Oracle source database.

## Project Info

| | |
|---|---|
| **Goal** | Maintain a full change history of customers using SCD2 |
| **Source** | Oracle DB (`customers`, `orders`) |
| **Target** | `dim_customer` (SCD2 dimension) |
| **Dataset size** | `200K` customers, `600K` orders |
| **Load type** | One-time initial load, then incremental via watermark |

## Tech Stack

- Python `3.14.6`
- PySpark `4.1.2`
- Oracle DB `23.26.2.0.0` + Oracle JDBC driver 

## Prerequisites

- Oracle instance running locally or reachable
- PySpark installed
- Oracle JDBC jar available on the Spark classpath
- DB credentials set as environment variables (`ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`)

## Execution Flow

1. Create a separate Oracle user and password for the project
2. Create the Oracle tables
3. Load source data: `initial-load.py`
4. Fix `customer_id` values in `orders` (id range mismatch with `customers`)
5. Alter `customers`: add `last_updated` and `is_deleted`
6. Create `dim_customer`
7. Create index on `dim_customer (customer_id, is_current)`
8. Create `etl_watermark`
9. Initial dimension load: `initial-load-dim_customer.py`
10. Verify `dim_customer` against `customers`
11. Simulate source changes: `simulate-changes.py`
12. Run the incremental SCD2 merge: `incremental_scd2_customers.py`

## SCD2 Design

| Column | Purpose |
|---|---|
| `effective_start` | When this version became active |
| `effective_end` | When this version was closed (open-ended for current row) |
| `is_current` | `1` for the active version, `0` for history |

- **Change detection:** `<hash of tracked columns / column-by-column compare>`
- **Incremental pickup:** `last_updated` compared against `etl_watermark`
- **Deletes:** soft deletes via `is_deleted`

## Concepts Covered

- Slowly Changing Dimension Type 2
- Initial vs incremental loads
- Watermark-based CDC
- Soft deletes
- Change detection
- Indexing for dimension lookups
- Spark JDBC read/write with Oracle
- Data reconciliation between source and dimension

## Repo Structure

```
.
├── initial-load.py
├── initial-load-dim_customer.py
├── simulate-changes.py
├── incremental_scd2_customers.py
├── oracle-init.sql
└── README.md
```