CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock_quantity INT
) USING DELTA;