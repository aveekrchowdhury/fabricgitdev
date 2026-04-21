CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    customer_name STRING,
    email STRING,
    phone STRING,
    created_date TIMESTAMP
) USING DELTA;