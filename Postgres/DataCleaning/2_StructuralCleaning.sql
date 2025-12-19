--2) Structural Cleaning

--- (2.1) orders table----

-- Normalize empty strings, trims, collapse spaces---

UPDATE staging.orders
SET
	order_id        = NULLIF(REGEXP_REPLACE(TRIM(order_id), '\s+', ' ', 'g'), ''),
	customer_id     = NULLIF(REGEXP_REPLACE(TRIM(customer_id), '\s+', ' ', 'g'), ''),
	order_status    = LOWER(NULLIF(REGEXP_REPLACE(TRIM(order_status), '\s+', ' ', 'g'), '')),

	order_purchase_timestamp     = NULLIF(REGEXP_REPLACE(TRIM(order_purchase_timestamp), '\s+', ' ', 'g'), ''),
	order_approved_at            = NULLIF(REGEXP_REPLACE(TRIM(order_approved_at), '\s+', ' ', 'g'), ''),
	order_delivered_carrier_date = NULLIF(REGEXP_REPLACE(TRIM(order_delivered_carrier_date), '\s+', ' ', 'g'), ''),
	order_delivered_customer_date = NULLIF(REGEXP_REPLACE(TRIM(order_delivered_customer_date), '\s+', ' ', 'g'), ''),
	order_estimated_delivery_date = NULLIF(REGEXP_REPLACE(TRIM(order_estimated_delivery_date), '\s+', ' ', 'g'), '');

---(2.2) customers table-----

UPDATE staging.customers
SET
	customer_id              = NULLIF(REGEXP_REPLACE(TRIM(customer_id), '\s', ' ', 'g'), ''),
	customer_unique_id       = NULLIF(REGEXP_REPLACE(TRIM(customer_unique_id), '\s', ' ', 'g'), ''),
	customer_zip_code_prefix = NULLIF(REGEXP_REPLACE(TRIM(customer_zip_code_prefix), '\s', ' ', 'g'), ''),
	customer_city            = LOWER(NULLIF(REGEXP_REPLACE(TRIM(customer_city), '\s', ' ', 'g'), '')),
	customer_state           = UPPER(NULLIF(REGEXP_REPLACE(TRIM(customer_state), '\s', ' ', 'g'), ''));
	
---(2.3) reviews table---

UPDATE staging.reviews
SET
	review_id = NULLIF(REGEXP_REPLACE(TRIM(review_id), '\s', ' ', 'g'), ''),
	order_id = NULLIF(REGEXP_REPLACE(TRIM(order_id), '\s', ' ', 'g'), ''),
	review_score = NULLIF(REGEXP_REPLACE(TRIM(review_score), '\s', '', 'g'), ''),
	review_comment_title = NULLIF(REGEXP_REPLACE(TRIM(review_comment_title), '\s', ' ', 'g'), ''),
	review_comment_message = NULLIF(REGEXP_REPLACE(TRIM(review_comment_message), '\s', ' ', 'g'), ''),
	review_creation_date = NULLIF(REGEXP_REPLACE(TRIM(review_creation_date), '\s', ' ', 'g'), ''),
	review_answer_timestamp = NULLIF(REGEXP_REPLACE(TRIM(review_answer_timestamp), '\s', ' ', 'g'), '');
	
----(2.4) order_payments-----

UPDATE staging.order_payments
SET
	order_id = NULLIF(REGEXP_REPLACE(TRIM(order_id), '\s', ' ', 'g'), ''),
	payment_sequential = NULLIF(REGEXP_REPLACE(TRIM(payment_sequential), '\s', '', 'g'), ''),
	payment_type = NULLIF(REGEXP_REPLACE(TRIM(payment_type), '\s', ' ', 'g'), ''),
	payment_installments = NULLIF(REGEXP_REPLACE(TRIM(payment_installments), '\s', '', 'g'), ''),
	payment_value = NULLIF(REGEXP_REPLACE(TRIM(payment_value), '\s', '', 'g'), '');



-----(2.5) order_items table----

UPDATE staging.order_items
SET
	order_id = NULLIF(REGEXP_REPLACE(TRIM(order_id), '\s', ' ', 'g'), ''),
	order_item_id = NULLIF(REGEXP_REPLACE(TRIM(order_item_id), '\s', '', 'g'), ''),
	product_id  = NULLIF(REGEXP_REPLACE(TRIM(product_id ), '\s', ' ', 'g'), ''),
	seller_id = NULLIF(REGEXP_REPLACE(TRIM(seller_id), '\s', ' ', 'g'), ''),
	shipping_limit_date = NULLIF(REGEXP_REPLACE(TRIM(shipping_limit_date), '\s', ' ', 'g'), ''),
	price = NULLIF(REGEXP_REPLACE(TRIM(price), '\s', '', 'g'), ''),
	freight_value = NULLIF(REGEXP_REPLACE(TRIM(freight_value), '\s', '', 'g'), '');
	



---(2.6) sellers table-----

UPDATE staging.sellers
SET
	seller_id = NULLIF(REGEXP_REPLACE(TRIM(seller_id), '\s', ' ', 'g'), ''),
	seller_zip_code_prefix = NULLIF(REGEXP_REPLACE(TRIM(seller_zip_code_prefix), '\s', ' ', 'g'), ''),
	seller_city = LOWER(NULLIF(REGEXP_REPLACE(TRIM(seller_city), '\s', ' ', 'g'), '')),
	seller_state = UPPER(NULLIF(REGEXP_REPLACE(TRIM(seller_state), '\s', ' ', 'g'), ''));
	
	
----(2.7) geolocation table----

UPDATE staging.geolocation
SET
	geolocation_zip_code_prefix = NULLIF(REGEXP_REPLACE(TRIM(geolocation_zip_code_prefix), '\s', ' ', 'g'), ''),
	geolocation_lat = NULLIF(REGEXP_REPLACE(TRIM(geolocation_lat), '\s', '', 'g'), ''),
	geolocation_lng = NULLIF(REGEXP_REPLACE(TRIM(geolocation_lng), '\s', '', 'g'), ''),
	geolocation_city = LOWER(NULLIF(REGEXP_REPLACE(TRIM(geolocation_city), '\s', ' ', 'g'), '')),
	geolocation_state = UPPER(NULLIF(REGEXP_REPLACE(TRIM(geolocation_state), '\s', ' ', 'g'), ''));
	

---- (2.8) product_category_name_translation table----

UPDATE staging.product_category_name_translation
	SET
		product_category_name = LOWER(NULLIF(REGEXP_REPLACE(TRIM(product_category_name), '\s', ' ', 'g'), '')),
		product_category_name_english = LOWER(NULLIF(REGEXP_REPLACE(TRIM(product_category_name_english), '\s', ' ', 'g'), ''));
		
---- (2.9) products -----

UPDATE staging.products
	SET
		product_id = NULLIF(REGEXP_REPLACE(TRIM(product_id), '\s', ' ', 'g'), ''),
		product_category_name = NULLIF(REGEXP_REPLACE(TRIM(product_category_name), '\s', ' ', 'g'), ''),
		product_name_lenght = NULLIF(REGEXP_REPLACE(TRIM(product_name_lenght), '\s', '', 'g'), ''),
		product_description_lenght = NULLIF(REGEXP_REPLACE(TRIM(product_description_lenght), '\s', '', 'g'), ''),
		product_photos_qty = NULLIF(REGEXP_REPLACE(TRIM(product_photos_qty), '\s', '', 'g'), ''),
		product_weight_g = NULLIF(REGEXP_REPLACE(TRIM(product_weight_g), '\s', '', 'g'), ''),
		product_length_cm = NULLIF(REGEXP_REPLACE(TRIM(product_length_cm), '\s', '', 'g'), ''),
		product_height_cm = NULLIF(REGEXP_REPLACE(TRIM(product_height_cm), '\s', '', 'g'), ''),
		product_width_cm = NULLIF(REGEXP_REPLACE(TRIM(product_width_cm), '\s', '', 'g'), '');
		
		
--2.i) Deduplication

--2.i.1) orders table---

WITH order_ranked AS (
	SELECT 
			_row_id,
			row_number() OVER (
						PARTITION BY order_id
						ORDER BY
								(CASE WHEN order_approved_at IS NULL THEN 0 ELSE 1 END +
								CASE WHEN order_delivered_carrier_date IS NULL THEN 0 ELSE 1 END +
								CASE WHEN order_delivered_customer_date IS NULL THEN 0 ELSE 1 END +
								CASE WHEN order_estimated_delivery_date IS NULL THEN 0 ELSE 1 END) DESC,
							    
								staging.try_parse_timestamp(order_purchase_timestamp) DESC
							
						) AS rn
		FROM staging.orders
)

DELETE FROM staging.orders
WHERE _row_id IN
				(
					SELECT _row_id
					FROM order_ranked
					WHERE rn >1
				);

--2.i.2) customers

WITH customer_ranked AS (
	SELECT _row_id,
	row_number() over(
						PARTITION BY customer_id
						ORDER BY 
							(CASE WHEN customer_unique_id IS NOT NULL THEN 1 ELSE 0 END +
							 CASE WHEN customer_zip_code_prefix IS NOT NULL THEN 1 ELSE 0 END +
							 CASE WHEN customer_city IS NOT NULL THEN 1 ELSE 0 END +
							 CASE WHEN customer_state IS NOT NULL THEN 1 ELSE 0 END) DESC
					) AS rn
	FROM staging.customers
)

DELETE FROM staging.customers
WHERE _row_id IN (
	SELECT _row_id 
	FROM customer_ranked
	WHERE rn > 1
);

--2.i.3) reviews table

DELETE FROM staging.reviews
WHERE _row_id IN (
	SELECT _row_id FROM (
		SELECT _row_id,
			ROW_NUMBER() OVER (PARTITION BY review_id
								ORDER BY staging.try_parse_timestamp(review_creation_date) DESC)
			AS rn
		FROM staging.reviews
	) t
	WHERE rn >1
);

--2.i.4) order items Pk = order_id, order_item_id

WITH order_items_ranked AS (
    SELECT 
        _row_id,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY
                (
                    (CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN seller_id IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN shipping_limit_date IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN freight_value IS NOT NULL THEN 1 ELSE 0 END)
                ) DESC,
                staging.try_parse_timestamp(shipping_limit_date) DESC
        ) AS rn
    FROM staging.order_items
)

DELETE FROM staging.order_items
WHERE _row_id IN (
    SELECT _row_id
    FROM order_items_ranked
    WHERE rn > 1
);
		
--2.i.5) Products		
		
WITH products_ranked AS (
    SELECT 
        _row_id,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                (
                    CASE WHEN product_category_name IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(product_name_lenght) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(product_description_lenght) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(product_photos_qty) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(product_weight_g) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(product_length_cm) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(product_height_cm) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(product_width_cm) IS NOT NULL THEN 1 ELSE 0 END
                ) DESC

        ) AS rn
    FROM staging.products
)

DELETE FROM staging.products
WHERE _row_id IN (
    SELECT _row_id
    FROM products_ranked
    WHERE rn > 1
);

--2.i.6) sellers			
		
WITH sellers_ranked AS (
    SELECT 
        _row_id,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY
                (
                    CASE WHEN seller_zip_code_prefix IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN seller_city IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN seller_state IS NOT NULL THEN 1 ELSE 0 END
                ) DESC

        ) AS rn
    FROM staging.sellers
)

DELETE FROM staging.sellers
WHERE _row_id IN (
    SELECT _row_id
    FROM sellers_ranked
    WHERE rn > 1
);

--2.i.7) geolocation
WITH geo_ranked AS (
    SELECT
        _row_id,
        ROW_NUMBER() OVER (
            PARTITION BY 
                geolocation_zip_code_prefix,
                staging.try_parse_numeric(geolocation_lat),
                staging.try_parse_numeric(geolocation_lng)
            ORDER BY
                -- completeness score
                (
                    CASE WHEN staging.try_parse_numeric(geolocation_lat) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_numeric(geolocation_lng) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN geolocation_city  IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN geolocation_state IS NOT NULL THEN 1 ELSE 0 END
                ) DESC

        ) AS rn
    FROM staging.geolocation
)

DELETE FROM staging.geolocation
WHERE _row_id IN (
    SELECT _row_id
    FROM geo_ranked
    WHERE rn > 1
);

--2.i.8) product_category_name_translation
WITH category_ranked AS (
    SELECT
        _row_id,
        ROW_NUMBER() OVER (
            PARTITION BY product_category_name
            ORDER BY
                -- completeness score
                (CASE WHEN product_category_name_english IS NOT NULL THEN 1 ELSE 0 END) DESC
        ) AS rn
    FROM staging.product_category_name_translation
)

DELETE FROM staging.product_category_name_translation
WHERE _row_id IN (
    SELECT _row_id
    FROM category_ranked
    WHERE rn > 1
);

--2.i.9) order_payments PK order_id, payment_sequential
WITH payments_ranked AS (
    SELECT 
        _row_id,
        ROW_NUMBER() OVER (
            PARTITION BY 
                order_id,
                staging.try_parse_int(payment_sequential)
            ORDER BY
                -- completeness score
                (
                    CASE WHEN payment_type IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_int(payment_installments) IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN staging.try_parse_numeric(payment_value) IS NOT NULL THEN 1 ELSE 0 END
                ) DESC
        ) AS rn
    FROM staging.order_payments
)

DELETE FROM staging.order_payments
WHERE _row_id IN (
    SELECT _row_id
    FROM payments_ranked
    WHERE rn > 1
);