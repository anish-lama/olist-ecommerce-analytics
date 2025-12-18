-----Update bronze to staging------

TRUNCATE staging.order_items CASCADE;
TRUNCATE staging.sellers CASCADE;
TRUNCATE staging.products CASCADE;
TRUNCATE staging.order_payments CASCADE;
TRUNCATE staging.reviews CASCADE;
TRUNCATE staging.orders CASCADE;
TRUNCATE staging.product_category_name_translation CASCADE;
TRUNCATE staging.geolocation CASCADE;
TRUNCATE staging.customers CASCADE;

INSERT INTO staging.customers
	SELECT * FROM bronze.customers;
INSERT INTO staging.orders
	SELECT * FROM bronze.orders;
INSERT INTO staging.reviews
	SELECT * FROM bronze.reviews;
INSERT INTO staging.order_payments
	SELECT * FROM bronze.order_payments;
INSERT INTO staging.product_category_name_translation
	SELECT * FROM bronze.product_category_name_translation;
INSERT INTO staging.products
	SELECT * FROM bronze.products;
INSERT INTO staging.sellers
	SELECT * FROM bronze.sellers;
INSERT INTO staging.order_items
	SELECT * FROM bronze.order_items;
INSERT INTO staging.geolocation
	SELECT * FROM bronze.geolocation;
