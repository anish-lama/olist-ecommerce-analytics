----3 Handle missing values------

--3.1) orders table---


INSERT INTO silver.missing_values_logs (
table_name, column_name, row_id
)
SELECT 
	'orders',
	'order_id',
	_row_id
FROM staging.orders
WHERE order_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'orders',
	'customer_id',
	_row_id
FROM staging.orders
WHERE customer_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'orders',
	'order_status',
	_row_id
FROM staging.orders
WHERE order_status IS NULL;


INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'orders',
	'order_purchase_timestamp',
	_row_id
FROM staging.orders
WHERE order_purchase_timestamp IS NULL;

--3.2) customers table ----

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'customers',
	'customer_id',
	_row_id
FROM staging.customers
WHERE customer_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'customers',
	'customer_unique_id',
	_row_id
FROM staging.customers
WHERE customer_unique_id IS NULL;

--3.3) reviews table ---

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'reviews',
	'review_id',
	_row_id
FROM staging.reviews
WHERE review_id IS NULL;


INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'reviews',
	'order_id',
	_row_id
FROM staging.reviews
WHERE order_id IS NULL;


INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'reviews',
	'review_score',
	_row_id
FROM staging.reviews
WHERE review_score IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'reviews',
	'review_creation_date',
	_row_id
FROM staging.reviews
WHERE review_creation_date IS NULL;

--3.4) order_payments table--


INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'order_payments',
	'order_id',
	_row_id
FROM staging.order_payments
WHERE order_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'order_payments',
	'payment_type',
	_row_id
FROM staging.order_payments
WHERE payment_type IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'order_payments',
	'payment_value',
	_row_id
FROM staging.order_payments
WHERE payment_value IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'order_payments',
	'payment_sequential',
	_row_id
FROM staging.order_payments
WHERE payment_sequential IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'order_payments',
	'payment_installments',
	_row_id
FROM staging.order_payments
WHERE payment_installments IS NULL;

---3.5) order_items ---

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'order_items',
	'order_id',
	_row_id
FROM staging.order_items
WHERE order_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'order_items',
	'order_item_id',
	_row_id
FROM staging.order_items
WHERE order_item_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'order_items',
	'product_id',
	_row_id
FROM staging.order_items
WHERE product_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'order_items',
	'seller_id',
	_row_id
FROM staging.order_items
WHERE seller_id IS NULL;


INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'order_items',
	'price',
	_row_id
FROM staging.order_items
WHERE price IS NULL;


---3.6) products ---
INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'products',
	'product_id',
	_row_id
FROM staging.products
WHERE product_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'products',
	'product_weight_g',
	_row_id
FROM staging.products
WHERE product_weight_g IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'products',
	'product_length_cm',
	_row_id
FROM staging.products
WHERE product_length_cm IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'products',
	'product_height_cm',
	_row_id
FROM staging.products
WHERE product_height_cm IS NULL;


INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT
	'products',
	'product_width_cm',
	_row_id
FROM staging.products
WHERE product_width_cm IS NULL;

---3.7) sellers ---
INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'sellers',
	'seller_id',
	_row_id
FROM staging.sellers
WHERE seller_id IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'sellers',
	'seller_zip_code_prefix',
	_row_id
FROM staging.sellers
WHERE seller_zip_code_prefix IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'sellers',
	'seller_city',
	_row_id
FROM staging.sellers
WHERE seller_city IS NULL;

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'sellers',
	'seller_state',
	_row_id
FROM staging.sellers
WHERE seller_state IS NULL;

---3.8) geolocation ---

INSERT INTO silver.missing_values_logs(
table_name,
column_name,
row_id
)
SELECT 
	'geolocation',
	'geolocation_zip_code_prefix',
	_row_id
FROM staging.geolocation
WHERE geolocation_zip_code_prefix IS NULL;

INSERT INTO silver.missing_values_logs(
table_name, 
column_name, 
row_id
)
SELECT 
	'geolocation', 
	'geolocation_lat', 
	_row_id
FROM staging.geolocation
WHERE geolocation_lat IS NULL;

INSERT INTO silver.missing_values_logs(
table_name, 
column_name, 
row_id
)
SELECT 
	'geolocation', 
	'geolocation_lng', 
	_row_id
FROM staging.geolocation
WHERE geolocation_lng IS NULL;

INSERT INTO silver.missing_values_logs(
table_name, 
column_name, 
row_id
)
SELECT 
	'geolocation', 
	'geolocation_city', 
	_row_id
FROM staging.geolocation
WHERE geolocation_city IS NULL;

INSERT INTO silver.missing_values_logs(
table_name, 
column_name, 
row_id
)
SELECT 
	'geolocation', 
	'geolocation_state', 
	_row_id
FROM staging.geolocation
WHERE geolocation_state IS NULL;

--3.9) product_category_name_translation

-- category name missing
INSERT INTO silver.missing_values_logs(
table_name, 
column_name, 
row_id
)
SELECT 
	'product_category_name_translation', 
	'product_category_name', 
	_row_id
FROM staging.product_category_name_translation
WHERE product_category_name IS NULL;

-- english name missing
INSERT INTO silver.missing_values_logs(
table_name, 
column_name, 
row_id)
SELECT 
	'product_category_name_translation', 
	'product_category_name_english', 
	_row_id
FROM staging.product_category_name_translation
WHERE product_category_name_english IS NULL;


SELECT * FROM silver.datatype_validation_errors_log;

SELECT * FROM silver.missing_values_logs;