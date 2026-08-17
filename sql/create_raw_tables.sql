-- ============================================================================
-- Milestone 1: Create Raw Tables
-- ============================================================================
-- Run these statements in your Snowflake worksheet.
-- Make sure you've set your database, schema, and warehouse first:
--
  USE WAREHOUSE FOX_WH;
  USE DATABASE FOX_DB;
  USE SCHEMA RAW_EXT;
--
-- The processor will automatically create the stages (orders_stage,
-- order_details_stage, chat_stage) when it runs, so you only need
-- to create the tables below.
-- ============================================================================

-- Orders from PostgreSQL (sales order headers)
CREATE TABLE IF NOT EXISTS orders_raw (
    sales_order_id VARCHAR,
    revision_number INT,
    status VARCHAR,
    online_order_flag BOOLEAN,
    sales_order_number VARCHAR,
    purchase_order_number VARCHAR,
    account_number VARCHAR,
    customer_id VARCHAR,
    sales_person_id VARCHAR,
    territory_id VARCHAR,
    bill_to_address_id VARCHAR,
    ship_to_address_id VARCHAR,
    ship_method_id VARCHAR,
    credit_card_id VARCHAR,
    credit_card_approval_code VARCHAR,
    currency_rate_id VARCHAR,
    sub_total DECIMAL(18, 2),
    tax_amt DECIMAL(18, 2),
    freight DECIMAL(18, 2),
    total_due DECIMAL(18, 2),
    comment VARCHAR,
    due_date TIMESTAMP,
    order_date TIMESTAMP,
    ship_date TIMESTAMP,
    last_modified TIMESTAMP
);

-- Order line items from PostgreSQL
CREATE TABLE IF NOT EXISTS order_details_raw (
    sales_order_detail_id VARCHAR,
    sales_order_id VARCHAR,
    carrier_tracking_number VARCHAR,
    order_qty INT,
    product_id VARCHAR,
    special_offer_id VARCHAR,
    unit_price DECIMAL(18, 2),
    unit_price_discount DECIMAL(18, 2),
    line_total DECIMAL(18, 2),
    last_modified TIMESTAMP
);

-- Chat logs from MongoDB (stored as semi-structured VARIANT)
CREATE TABLE IF NOT EXISTS chat_logs_raw (
    raw VARIANT
);


SELECT COUNT(*) FROM raw_ext.orders_raw;
SELECT COUNT(*) FROM raw_ext.order_details_raw;
SELECT COUNT(*) FROM raw_ext.chat_logs_raw;
LIST @orders_stage;
LIST @order_details_stage;
LIST @chat_stage;

SELECT order_details 
FROM dbt_dev.stg_ecom__sales_orders 
LIMIT 1;

SELECT column_name 
FROM information_schema.columns 
WHERE table_schema = 'RAW_EXT' 
AND table_name IN ('ORDERS_RAW', 'ORDER_DETAILS_RAW', 'CHAT_LOGS_RAW')
ORDER BY table_name, ordinal_position;

SELECT column_name 
FROM information_schema.columns 
WHERE table_schema = 'RAW_EXT' 
AND table_name = 'CHAT_LOGS_RAW'
ORDER BY ordinal_position;

SELECT online_order_flag FROM dbt_dev.base_ecom__sales_orders LIMIT 1;

SELECT *
FROM dbt_dev.base_real_time__sales_orders
WHERE TO_VARCHAR(status) = 'NaN'
   OR TO_VARCHAR(sub_total) = 'NaN'
   OR TO_VARCHAR(tax_amt) = 'NaN'
   OR TO_VARCHAR(total_due) = 'NaN'
   OR TO_VARCHAR(freight) = 'NaN'
   OR TO_VARCHAR(online_order_flag) = 'NaN'
   OR TO_VARCHAR(ship_method_id) = 'NaN'
LIMIT 5;

SELECT *
FROM dbt_dev.base_ecom__sales_orders
WHERE TO_VARCHAR(status) = 'NaN'
   OR TO_VARCHAR(sub_total) = 'NaN'
   OR TO_VARCHAR(tax_amt) = 'NaN'
   OR TO_VARCHAR(total_due) = 'NaN'
   OR TO_VARCHAR(freight) = 'NaN'
   OR TO_VARCHAR(online_order_flag) = 'NaN'
   OR TO_VARCHAR(ship_method_id) = 'NaN'
LIMIT 5;

SELECT *
FROM dbt_dev.base_ecom__sales_orders
WHERE sales_order_id = 'NaN'
   OR customer_id = 'NaN'
   OR account_number = 'NaN'
   OR delivery_estimate = 'NaN'
LIMIT 5;

SELECT * FROM dbt_dev.base_ecom__sales_orders
UNION ALL
SELECT * FROM dbt_dev.base_real_time__sales_orders;

SELECT bill_to_address_id FROM dbt_dev.base_ecom__sales_orders
UNION ALL
SELECT bill_to_address_id FROM dbt_dev.base_real_time__sales_orders;

SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'DBT_DEV' 
AND table_name IN ('BASE_ECOM__SALES_ORDERS', 'BASE_REAL_TIME__SALES_ORDERS')
ORDER BY table_name, ordinal_position;

SELECT COUNT(*) FROM raw_ext.orders_raw;