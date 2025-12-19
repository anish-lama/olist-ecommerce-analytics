CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-------Bronze--------------
DROP TABLE IF EXISTS bronze.order_items;
DROP TABLE IF EXISTS bronze.sellers;
DROP TABLE IF EXISTS bronze.products;
DROP TABLE IF EXISTS bronze.order_payments;
DROP TABLE IF EXISTS bronze.reviews;
DROP TABLE IF EXISTS bronze.orders;
DROP TABLE IF EXISTS bronze.product_category_name_translation;
DROP TABLE IF EXISTS bronze.geolocation;
DROP TABLE IF EXISTS bronze.customers;
DROP TABLE IF EXISTS staging.expected_column_types;

CREATE TABLE bronze.customers (
	customer_id TEXT,
	customer_unique_id TEXT,
	customer_zip_code_prefix TEXT,
	customer_city TEXT,
	customer_state TEXT
);

CREATE TABLE bronze.geolocation (
	geolocation_zip_code_prefix TEXT,
	geolocation_lat TEXT,
	geolocation_lng TEXT,
	geolocation_city TEXT,
	geolocation_state TEXT
);

CREATE TABLE bronze.product_category_name_translation (
	product_category_name TEXT,
	product_category_name_english TEXT
);

CREATE TABLE bronze.orders (
	order_id TEXT,
	customer_id TEXT,
	order_status TEXT,
	order_purchase_timestamp TEXT,
	order_approved_at TEXT,
	order_delivered_carrier_date TEXT,
	order_delivered_customer_date TEXT,
	order_estimated_delivery_date TEXT
);

CREATE TABLE bronze.reviews (
	review_id TEXT,
	order_id TEXT,
	review_score INT,
	review_comment_title TEXT,
	review_comment_message TEXT,
	review_creation_date TEXT,
	review_answer_timestamp TEXT
);

CREATE TABLE bronze.order_payments (
	order_id TEXT,
	payment_sequential TEXT,
	payment_type TEXT,
	payment_installments TEXT,
	payment_value TEXT
);


CREATE TABLE bronze.products (
	product_id TEXT,
	product_category_name TEXT,
	product_name_lenght TEXT,
	product_description_lenght TEXT,
	product_photos_qty TEXT,
	product_weight_g TEXT,
	product_length_cm TEXT,
	product_height_cm TEXT,
	product_width_cm TEXT
);

CREATE TABLE bronze.sellers (
	seller_id TEXT,
	seller_zip_code_prefix TEXT,
	seller_city TEXT,
	seller_state TEXT
);

CREATE TABLE bronze.order_items (
	order_id TEXT,
	order_item_id TEXT,
	product_id TEXT,
	seller_id TEXT,
	shipping_limit_date TEXT,
	price TEXT,
	freight_value TEXT
);

-------Staging-------------
DROP TABLE IF EXISTS staging.order_items;
DROP TABLE IF EXISTS staging.sellers;
DROP TABLE IF EXISTS staging.products;
DROP TABLE IF EXISTS staging.order_payments;
DROP TABLE IF EXISTS staging.reviews;
DROP TABLE IF EXISTS staging.orders;
DROP TABLE IF EXISTS staging.product_category_name_translation;
DROP TABLE IF EXISTS staging.geolocation;
DROP TABLE IF EXISTS staging.customers;

CREATE TABLE staging.customers (
	customer_id TEXT,
	customer_unique_id TEXT,
	customer_zip_code_prefix TEXT,
	customer_city TEXT,
	customer_state TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);

CREATE TABLE staging.geolocation (
	geolocation_zip_code_prefix TEXT,
	geolocation_lat TEXT,
	geolocation_lng TEXT,
	geolocation_city TEXT,
	geolocation_state TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);

CREATE TABLE staging.product_category_name_translation (
	product_category_name TEXT,
	product_category_name_english TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);

CREATE TABLE staging.orders (
	order_id TEXT,
	customer_id TEXT,
	order_status TEXT,
	order_purchase_timestamp TEXT,
	order_approved_at TEXT,
	order_delivered_carrier_date TEXT,
	order_delivered_customer_date TEXT,
	order_estimated_delivery_date TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);

CREATE TABLE staging.reviews (
	review_id TEXT,
	order_id TEXT,
	review_score TEXT,
	review_comment_title TEXT,
	review_comment_message TEXT,
	review_creation_date TEXT,
	review_answer_timestamp TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);

CREATE TABLE staging.order_payments (
	order_id TEXT,
	payment_sequential TEXT,
	payment_type TEXT,
	payment_installments TEXT,
	payment_value TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);


CREATE TABLE staging.products (
	product_id TEXT,
	product_category_name TEXT,
	product_name_lenght TEXT,
	product_description_lenght TEXT,
	product_photos_qty TEXT,
	product_weight_g TEXT,
	product_length_cm TEXT,
	product_height_cm TEXT,
	product_width_cm TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);

CREATE TABLE staging.sellers (
	seller_id TEXT,
	seller_zip_code_prefix TEXT,
	seller_city TEXT,
	seller_state TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);


CREATE TABLE staging.order_items (
	order_id TEXT,
	order_item_id TEXT,
	product_id TEXT,
	seller_id TEXT,
	shipping_limit_date TEXT,
	price TEXT,
	freight_value TEXT,
	_row_id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);



CREATE TABLE IF NOT EXISTS staging.expected_column_types (
	table_name TEXT NOT NULL,
	column_name TEXT NOT NULL,
	expected_type TEXT NOT NULL,
	PRIMARY KEY (table_name, column_name)
);

INSERT INTO staging.expected_column_types (table_name, column_name, expected_type) VALUES
-- CUSTOMERS
('customers', 'customer_id', 'TEXT'),
('customers', 'customer_unique_id', 'TEXT'),
('customers', 'customer_zip_code_prefix', 'TEXT'),
('customers', 'customer_city', 'TEXT'),
('customers', 'customer_state', 'TEXT'),

-- GEOLOCATION
('geolocation', 'geolocation_zip_code_prefix', 'TEXT'),
('geolocation', 'geolocation_lat', 'NUMERIC'),
('geolocation', 'geolocation_lng', 'NUMERIC'),
('geolocation', 'geolocation_city', 'TEXT'),
('geolocation', 'geolocation_state', 'TEXT'),

-- PRODUCT CATEGORY NAME TRANSLATION
('product_category_name_translation', 'product_category_name', 'TEXT'),
('product_category_name_translation', 'product_category_name_english', 'TEXT'),

-- ORDERS
('orders', 'order_id', 'TEXT'),
('orders', 'customer_id', 'TEXT'),
('orders', 'order_status', 'TEXT'), 
('orders', 'order_purchase_timestamp', 'TIMESTAMP'),
('orders', 'order_approved_at', 'TIMESTAMP'),
('orders', 'order_delivered_carrier_date', 'TIMESTAMP'),
('orders', 'order_delivered_customer_date', 'TIMESTAMP'),
('orders', 'order_estimated_delivery_date', 'TIMESTAMP'),

-- REVIEWS
('reviews', 'review_id', 'TEXT'),
('reviews', 'order_id', 'TEXT'),
('reviews', 'review_score', 'INT'),
('reviews', 'review_comment_title', 'TEXT'),
('reviews', 'review_comment_message', 'TEXT'),
('reviews', 'review_creation_date', 'TIMESTAMP'),
('reviews', 'review_answer_timestamp', 'TIMESTAMP'),

-- ORDER PAYMENTS
('order_payments', 'order_id', 'TEXT'),
('order_payments', 'payment_sequential', 'INT'),
('order_payments', 'payment_type', 'TEXT'),
('order_payments', 'payment_installments', 'INT'),
('order_payments', 'payment_value', 'NUMERIC'),

-- PRODUCTS
('products', 'product_id', 'TEXT'),
('products', 'product_category_name', 'TEXT'),
('products', 'product_name_lenght', 'INT'),
('products', 'product_description_lenght', 'INT'),
('products', 'product_photos_qty', 'INT'),
('products', 'product_weight_g', 'INT'),
('products', 'product_length_cm', 'INT'),
('products', 'product_height_cm', 'INT'),
('products', 'product_width_cm', 'INT'),

-- SELLERS
('sellers', 'seller_id', 'TEXT'),
('sellers', 'seller_zip_code_prefix', 'TEXT'),
('sellers', 'seller_city', 'TEXT'),
('sellers', 'seller_state', 'TEXT'),

-- ORDER ITEMS
('order_items', 'order_id', 'TEXT'),
('order_items', 'order_item_id', 'INT'),
('order_items', 'product_id', 'TEXT'),
('order_items', 'seller_id', 'TEXT'),
('order_items', 'shipping_limit_date', 'TIMESTAMP'),
('order_items', 'price', 'NUMERIC'),
('order_items', 'freight_value', 'NUMERIC');



-------Silver--------------
DROP TABLE IF EXISTS silver.order_items CASCADE;
DROP TABLE IF EXISTS silver.sellers CASCADE;
DROP TABLE IF EXISTS silver.products CASCADE;
DROP TABLE IF EXISTS silver.order_payments CASCADE;
DROP TABLE IF EXISTS silver.reviews CASCADE;
DROP TABLE IF EXISTS silver.orders CASCADE;
DROP TABLE IF EXISTS silver.product_category_name_translation CASCADE;
DROP TABLE IF EXISTS silver.geolocation CASCADE;
DROP TABLE IF EXISTS silver.customers CASCADE;

CREATE TABLE silver.customers (
	customer_id TEXT PRIMARY KEY,
	customer_unique_id TEXT NOT NULL,
	customer_zip_code_prefix TEXT,
	customer_city TEXT,
	customer_state TEXT
);

CREATE TABLE silver.geolocation (
	geolocation_zip_code_prefix TEXT NOT NULL,
	geolocation_lat NUMERIC(10,2),
	geolocation_lng NUMERIC(10,2),
	geolocation_city TEXT,
	geolocation_state TEXT
);

CREATE TABLE silver.product_category_name_translation (
	product_category_name TEXT PRIMARY KEY,
	product_category_name_english TEXT
);

CREATE TABLE silver.orders (
	order_id TEXT PRIMARY KEY,
	customer_id TEXT NOT NULL REFERENCES silver.customers(customer_id),
	order_status TEXT NOT NULL,
	order_purchase_timestamp TIMESTAMPTZ NOT NULL,
	order_approved_at TIMESTAMPTZ,
	order_delivered_carrier_date TIMESTAMPTZ,
	order_delivered_customer_date TIMESTAMPTZ,
	order_estimated_delivery_date TIMESTAMPTZ
);

CREATE TABLE silver.reviews (
	review_id TEXT PRIMARY KEY,
	order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
	review_score INT NOT NULL,
	review_comment_title TEXT,
	review_comment_message TEXT,
	review_creation_date TIMESTAMPTZ NOT NULL,
	review_answer_timestamp TIMESTAMPTZ
);

CREATE TABLE silver.order_payments (
	order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
	payment_sequential INT,
	payment_type TEXT,
	payment_installments INT,
	payment_value NUMERIC(10,2),
	PRIMARY KEY (order_id, payment_sequential)
);


CREATE TABLE silver.products (
	product_id TEXT PRIMARY KEY,
	product_category_name TEXT,
	product_name_lenght TEXT,
	product_description_lenght INT,
	product_photos_qty INT,
	product_weight_g INT NOT NULL,
	product_length_cm INT NOT NULL,
	product_height_cm INT NOT NULL,
	product_width_cm INT NOT NULL
);

CREATE TABLE silver.sellers (
	seller_id TEXT NOT NULL PRIMARY KEY,
	seller_zip_code_prefix TEXT NOT NULL,
	seller_city TEXT NOT NULL,
	seller_state TEXT NOT NULL
);

CREATE TABLE silver.order_items (
	order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
	order_item_id INT NOT NULL,
	product_id TEXT NOT NULL REFERENCES silver.products(product_id),
	seller_id TEXT NOT NULL REFERENCES silver.sellers(seller_id),
	shipping_limit_date TIMESTAMPTZ,
	price NUMERIC(10,2) NOT NULL,
	freight_value NUMERIC(10,2),
	PRIMARY KEY (order_id, order_item_id)
);


--ERROR Log table for silver.orders---
CREATE TABLE silver.business_validation_errors_log (
error_code BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
table_name TEXT NOT NULL,
row_id UUID NOT NULL,
error_description TEXT NOT NULL,
failed_mismatched_column TEXT NOT NULL,
created_at TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'America/Sao_Paulo')
);



--ERROR Log table for dataype validation
CREATE TABLE IF NOT EXISTS silver.datatype_validation_errors_log (
    error_code BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Where error happened
    table_name      TEXT        NOT NULL,
    column_name     TEXT        NOT NULL,

    -- Unique identifier for the exact record (from staging)
    row_id          UUID        NOT NULL,

    -- The original invalid value
    invalid_value   TEXT,

    -- What datatype we expected (from staging.expected_column_types)
    expected_type   TEXT        NOT NULL,

    -- Regex or rule description that triggered the failure
    validation_rule TEXT        NOT NULL,

    -- Full snapshot of the row at time of error (important for traceability)
    row_data        JSONB       NOT NULL,

    -- Error timestamp
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

--ERROR Log table for missing values
CREATE TABLE IF NOT EXISTS silver.missing_values_logs (

	error_code BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

	table_name TEXT NOT NULL,
	column_name TEXT NOT NULL,

	row_id UUID NOT NULL,
	logged_at TIMESTAMPTZ NOT NULL DEFAULT now()
)


