-- Products
CREATE TABLE products (
    product_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         VARCHAR2(100) NOT NULL,
    category     VARCHAR2(50),
    price        NUMBER(10,2),
    created_date DATE DEFAULT SYSDATE
);

-- Customers
CREATE TABLE customers (
    customer_id  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name   VARCHAR2(50),
    last_name    VARCHAR2(50),
    email        VARCHAR2(100),
    city         VARCHAR2(50),
    state        VARCHAR2(30),
    signup_date  DATE DEFAULT SYSDATE
);

-- Orders
CREATE TABLE orders (
    order_id     NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  NUMBER REFERENCES customers(customer_id),
    order_date   DATE,
    status       VARCHAR2(20),
    total_amount NUMBER(12,2)
);

-- Order Items
CREATE TABLE order_items (
    item_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id     NUMBER REFERENCES orders(order_id),
    product_id   NUMBER REFERENCES products(product_id),
    quantity     NUMBER(5),
    unit_price   NUMBER(10,2),
    discount     NUMBER(5,2) DEFAULT 0
);

-- Analytics target table (Spark will write here)
CREATE TABLE daily_sales_summary (
    summary_date   DATE,
    category       VARCHAR2(50),
    total_orders   NUMBER,
    total_revenue  NUMBER(14,2),
    avg_order_value NUMBER(10,2),
    top_product    VARCHAR2(100),
    etl_timestamp  TIMESTAMP DEFAULT SYSTIMESTAMP
);

UPDATE orders
SET customer_id = customer_id + 222220;
COMMIT;

ALTER TABLE customers ADD (
  last_updated TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  is_deleted   NUMBER(1) DEFAULT 0 NOT NULL
);

-- SCD Type 2 customer dimension
CREATE TABLE dim_customer (
    customer_sk    NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- surrogate key, one per version
    customer_id    NUMBER NOT NULL, -- business key from source
    first_name     VARCHAR2(50),
    last_name      VARCHAR2(50),
    email          VARCHAR2(100),
    city           VARCHAR2(50),
    state          VARCHAR2(30),
    signup_date    DATE,
    is_deleted     NUMBER(1),
    effective_from TIMESTAMP     NOT NULL,
    effective_to   TIMESTAMP, -- NULL = open-ended (current row)
    is_current     NUMBER(1) NOT NULL
);

CREATE INDEX idx_dim_customer_bk ON dim_customer(customer_id, is_current);

-- One row per pipeline: where the last run stopped
CREATE TABLE etl_watermark (
    pipeline_name  VARCHAR2(50) PRIMARY KEY,
    last_watermark TIMESTAMP
);