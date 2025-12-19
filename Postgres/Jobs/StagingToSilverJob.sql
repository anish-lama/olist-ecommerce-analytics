TRUNCATE silver.order_items CASCADE;
TRUNCATE silver.sellers CASCADE;
TRUNCATE silver.products CASCADE;
TRUNCATE silver.order_payments CASCADE;
TRUNCATE silver.reviews CASCADE;
TRUNCATE silver.orders CASCADE;
TRUNCATE silver.product_category_name_translation CASCADE;
TRUNCATE silver.geolocation CASCADE;
TRUNCATE silver.customers CASCADE;

INSERT INTO silver.customers (
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
)
SELECT 
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
FROM staging.customers c
WHERE 
	c._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'customers'
	)
	AND c._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name ='customers'
	);

INSERT INTO silver.orders (
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date
)
SELECT
	o.order_id,
	o.customer_id,
	o.order_status,

	staging.try_parse_timestamp(o.order_purchase_timestamp)
		AT TIME ZONE 'America/Sao_Paulo',
		
	staging.try_parse_timestamp(o.order_approved_at)
		AT TIME ZONE 'America/Sao_Paulo',

	staging.try_parse_timestamp(o.order_delivered_carrier_date)
		AT TIME ZONE 'America/Sao_Paulo',	

	staging.try_parse_timestamp(o.order_delivered_customer_date)
		AT TIME ZONE 'America/Sao_Paulo',	

	staging.try_parse_timestamp(o.order_estimated_delivery_date)
		AT TIME ZONE 'America/Sao_Paulo'

FROM staging.orders o
WHERE

	o._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'orders'
	)

	AND o._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'orders'
	)
	AND o._row_id NOT IN (
	    SELECT row_id
	    FROM silver.business_validation_errors_log
	    WHERE table_name = 'orders'
	      AND error_description = 'fk_customer_not_found'
	);

INSERT INTO silver.geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
SELECT
    geolocation_zip_code_prefix,
    staging.try_parse_numeric(geolocation_lat),
    staging.try_parse_numeric(geolocation_lng),
    geolocation_city,
    geolocation_state
FROM staging.geolocation g
WHERE g._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log
    WHERE table_name = 'geolocation'
)
AND g._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs
    WHERE table_name = 'geolocation'
);

INSERT INTO silver.product_category_name_translation (
    product_category_name,
    product_category_name_english
)
SELECT
    product_category_name,
    product_category_name_english
FROM staging.product_category_name_translation t
WHERE t._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log
    WHERE table_name = 'product_category_name_translation'
)
AND t._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs
    WHERE table_name = 'product_category_name_translation'
);

INSERT INTO silver.reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
SELECT
    review_id,
    order_id,
    review_score::INT,
    review_comment_title,
    review_comment_message,
    review_creation_date::TIMESTAMPTZ AT TIME ZONE 'America/Sao_Paulo',
    review_answer_timestamp::TIMESTAMPTZ AT TIME ZONE 'America/Sao_Paulo'
FROM staging.reviews r
WHERE r._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log
    WHERE table_name = 'reviews'
)
AND r._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs
    WHERE table_name = 'reviews'
)
AND r._row_id NOT IN (
    SELECT row_id
    FROM silver.business_validation_errors_log
    WHERE table_name = 'reviews'
      AND error_description = 'fk_order_not_found'
);


INSERT INTO silver.reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
SELECT
    review_id,
    order_id,
    staging.try_parse_int(review_score),
    review_comment_title,
    review_comment_message,
    staging.try_parse_timestamp(review_creation_date)
        AT TIME ZONE 'America/Sao_Paulo',
    staging.try_parse_timestamp(review_answer_timestamp)
        AT TIME ZONE 'America/Sao_Paulo'
FROM staging.reviews r
WHERE r._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log
    WHERE table_name = 'reviews'
)
AND r._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs
    WHERE table_name = 'reviews'
);


INSERT INTO silver.order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
SELECT
    order_id,
    staging.try_parse_int(payment_sequential),
    payment_type,
    staging.try_parse_int(payment_installments),
    staging.try_parse_numeric(payment_value)
FROM staging.order_payments op
WHERE op._row_id NOT IN (
    SELECT row_id
    FROM silver.datatype_validation_errors_log
    WHERE table_name = 'order_payments'
)
AND op._row_id NOT IN (
    SELECT row_id
    FROM silver.missing_values_logs
    WHERE table_name = 'order_payments'
)
AND op._row_id NOT IN (
    SELECT row_id
    FROM silver.business_validation_errors_log
    WHERE table_name = 'order_payments'
      AND error_description = 'fk_order_not_found'
);


INSERT INTO silver.products (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT
    product_id,
    product_category_name,

    staging.try_parse_int(product_name_lenght),
    staging.try_parse_int(product_description_lenght),
    staging.try_parse_int(product_photos_qty),
    staging.try_parse_int(product_weight_g),
    staging.try_parse_int(product_length_cm),
    staging.try_parse_int(product_height_cm),
    staging.try_parse_int(product_width_cm)

FROM staging.products p
WHERE p._row_id NOT IN (
    SELECT row_id
    FROM silver.datatype_validation_errors_log
    WHERE table_name = 'products'
)
AND p._row_id NOT IN (
    SELECT row_id
    FROM silver.missing_values_logs
    WHERE table_name = 'products'
);

INSERT INTO silver.sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM staging.sellers s
WHERE s._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log
    WHERE table_name = 'sellers'
)
AND s._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs
    WHERE table_name = 'sellers'
);

INSERT INTO silver.order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
SELECT
    order_id,
    staging.try_parse_int(order_item_id),
    product_id,
    seller_id,
    staging.try_parse_timestamp(shipping_limit_date)
        AT TIME ZONE 'America/Sao_Paulo',
    staging.try_parse_numeric(price),
    staging.try_parse_numeric(freight_value)
FROM staging.order_items oi
WHERE oi._row_id NOT IN (
    SELECT row_id
    FROM silver.datatype_validation_errors_log
    WHERE table_name = 'order_items'
)
AND oi._row_id NOT IN (
    SELECT row_id
    FROM silver.missing_values_logs
    WHERE table_name = 'order_items'
)
AND oi._row_id NOT IN (
    SELECT row_id
    FROM silver.business_validation_errors_log
    WHERE table_name = 'order_items'
      AND error_description IN (
          'fk_order_not_found',
          'fk_product_not_found',
          'fk_seller_not_found'
      )
);



