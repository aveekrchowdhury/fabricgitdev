-- Fabric notebook source


-- CELL ********************

CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    customer_id INT,
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS order_items (
    item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
) USING DELTA;
