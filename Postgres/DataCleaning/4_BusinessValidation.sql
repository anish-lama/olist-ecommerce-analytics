----4 Business Validation------	


--(3.1) Orders table---

---Timing rule check-----

--Logging the wrong timing records----


--Rule 1: customer_delivery_date < order_delivered_carrier_date (I)

INSERT INTO silver.Business_validation_errors_log(
	table_name,
	row_id,
	error_description,
	failed_mismatched_column
)
SELECT
	'orders',
	_row_id,
	'customer_delivery_before_carrier',
	'order_delivered_carrier_date'
FROM staging.orders o
WHERE 
	staging.try_parse_timestamp(order_delivered_customer_date) 
	< staging.try_parse_timestamp(order_delivered_carrier_date)
	
	AND o._row_id NOT IN (
		SELECT row_id 
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'orders')
	
	AND o._row_id NOT IN (
		SELECT row_id 
		FROM silver.missing_values_logs
		WHERE table_name = 'orders');

--Rule 2: customer_delivery_date < order_approved_at (I)

INSERT INTO silver.Business_validation_errors_log(
	table_name,
	row_id,
	error_description,
	failed_mismatched_column
)
SELECT 
	'orders',
	o._row_id,
	'customer_delivery_before_order_approved',
	'order_approved_at'
FROM staging.orders o
WHERE 
	staging.try_parse_timestamp(order_delivered_customer_date) 
	< staging.try_parse_timestamp(order_approved_at)
	
	AND o._row_id NOT IN (
		SELECT row_id 
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'orders')
	
	AND o._row_id NOT IN (
		SELECT row_id 
		FROM silver.missing_values_logs
		WHERE table_name = 'orders');


--Rule 3: customer_delivery_date (I) < order_purchase_timestamp 

INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'customer_delivery_before_order_purchase',
    'order_delivered_customer_date'
FROM staging.orders o
WHERE 
    staging.try_parse_timestamp(order_delivered_customer_date)
        < staging.try_parse_timestamp(order_purchase_timestamp)

    -- Exclude rows already flagged as datatype invalid
    AND o._row_id NOT IN (
        SELECT row_id 
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude rows missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );


--Rule 4: order_delivered_carrier_date (I) < order_approved_at  

INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'carrier_delivery_before_order_approved',
    'order_delivered_carrier_date'
FROM staging.orders o
WHERE 
    staging.try_parse_timestamp(order_delivered_carrier_date)
        < staging.try_parse_timestamp(order_approved_at)

    -- Exclude datatype errors
    AND o._row_id NOT IN (
        SELECT row_id 
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );



--Rule 5: order_delivered_carrier_date (I) < order_purchase_timestamp  

INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'carrier_delivery_before_order_purchase',
    'order_delivered_carrier_date'
FROM staging.orders o
WHERE 
    staging.try_parse_timestamp(order_delivered_carrier_date)
        < staging.try_parse_timestamp(order_purchase_timestamp)

    -- Exclude datatype validation errors
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );



--Rule 6: order_approved_at (I) < order_purchase_timestamp  

INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_approved_before_order_purchase',
    'order_approved_at'
FROM staging.orders o
WHERE 
    staging.try_parse_timestamp(order_approved_at)
        < staging.try_parse_timestamp(order_purchase_timestamp)

    -- Exclude rows already flagged for datatype issues
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude rows missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );

SELECT * FROM silver.Business_validation_errors_log;

--Validate order_status column---

---Log if mismatched order_status
--CASE 1: Created

INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_creation_mismatch',
    'order_status'
FROM staging.orders o
WHERE 
    order_status = 'created'
    AND NOT (
        order_purchase_timestamp IS NOT NULL 
        AND order_approved_at IS NULL
        AND order_delivered_carrier_date IS NULL 
        AND order_delivered_customer_date IS NULL
    )

    -- Exclude datatype validation failures
    AND o._row_id NOT IN (
        SELECT row_id 
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory value failures
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );


--CASE 2: Approved
INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_approved_mismatch',
    'order_status'
FROM staging.orders o
WHERE 
    order_status = 'approved'
    AND NOT (
        order_purchase_timestamp IS NOT NULL 
        AND order_approved_at IS NOT NULL
        AND order_delivered_carrier_date IS NULL 
        AND order_delivered_customer_date IS NULL
    )

    -- Exclude datatype issues
    AND o._row_id NOT IN (
        SELECT row_id 
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id 
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );


--CASE 3: Invoiced
INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_invoiced_mismatch',
    'order_status'
FROM staging.orders o
WHERE 
    order_status = 'invoiced'
    AND NOT (
        order_purchase_timestamp IS NOT NULL 
        AND order_approved_at IS NOT NULL
        AND order_delivered_carrier_date IS NULL 
        AND order_delivered_customer_date IS NULL
    )

    -- Exclude datatype validation failures
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );


--CASE 4: Processing
INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_processing_mismatch',
    'order_status'
FROM staging.orders o
WHERE 
    order_status = 'processing'
    AND NOT (
        order_purchase_timestamp IS NOT NULL 
        AND order_approved_at IS NOT NULL
        AND order_delivered_carrier_date IS NULL 
        AND order_delivered_customer_date IS NULL
    )

    -- Exclude datatype validation errors
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory value errors
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );


--CASE 5: Shipped
INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_shipped_mismatch',
    'order_status'
FROM staging.orders o
WHERE 
    order_status = 'shipped'
    AND NOT (
        order_purchase_timestamp IS NOT NULL 
        AND order_approved_at IS NOT NULL
        AND order_delivered_carrier_date IS NOT NULL
        AND order_delivered_customer_date IS NULL
    )

    -- Exclude datatype validation failures
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory value failures
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );



--CASE 6: Delivered
INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_delivered_mismatch',
    'order_status'
FROM staging.orders o
WHERE 
    order_status = 'delivered'
    AND NOT (
        order_purchase_timestamp IS NOT NULL
        AND order_delivered_customer_date IS NOT NULL
    )

    -- Exclude datatype validation failures
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );



--CASE 7: Canceled
INSERT INTO silver.Business_validation_errors_log(
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'orders',
    o._row_id,
    'order_canceled_mismatch',
    'order_status'
FROM staging.orders o
WHERE 
    order_status = 'canceled'
    AND order_purchase_timestamp IS NULL

    -- Exclude datatype errors
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'orders'
    )

    -- Exclude missing mandatory values
    AND o._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'orders'
    );
--customers table--
--Case 1:  Missing customer zip code prefix
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'customers',
    c._row_id,
    'invalid_customer_zip_code_prefix',
    'customer_zip_code_prefix'
FROM staging.customers c
WHERE
    customer_zip_code_prefix IS NOT NULL
    AND (
        customer_zip_code_prefix !~ '^[0-9]{4,5}$'
    )

    -- Exclude datatype errors
    AND c._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'customers'
    )

    -- Exclude hard missing mandatory values
    AND c._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'customers'
    );

--Case 2: Missing customer_city
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'customers',
    c._row_id,
    'missing_customer_city',
    'customer_city'
FROM staging.customers c
WHERE
    customer_city IS NULL

    AND c._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'customers'
    )

    AND c._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'customers'
    );

--Case3: Invalid customer_state
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'customers',
    c._row_id,
    'invalid_customer_state',
    'customer_state'
FROM staging.customers c
WHERE
    customer_state IS NOT NULL
    AND customer_state NOT IN (
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
        'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
        'RS','RO','RR','SC','SP','SE','TO'
    )

    AND c._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'customers'
    )

    AND c._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'customers'
    );

--Order_items table---
SELECT * FROM staging.order_items
LIMIT 5; 

--Case 1: price < 0
INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'order_items',
	_row_id,
	'negative_price',
	'price'
FROM staging.order_items oi
WHERE staging.try_parse_int(price) < 0

	AND oi._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'order_items'
	)

	AND oi._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'order_items'
	);


--Case 2: freight_value < 0
INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'order_items',
	_row_id,
	'negative_freight_value',
	'freight_Value'
FROM staging.order_items oi
WHERE staging.try_parse_int(freight_value) < 0
	AND oi._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'order_items'
	)

	AND oi._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'order_items'
	);


--Case 3 shippping_limit_date < order_purchase_timestamp

INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT 
	'order_items',
	oi._row_id,
	'shipping_limit_date_before_order_purchase',
	'shipping_limit_date'
FROM staging.order_items oi
JOIN staging.orders o
ON oi.order_id = o.order_id
WHERE staging.try_parse_timestamp(oi.shipping_limit_date) <
	staging.try_parse_timestamp(o.order_purchase_timestamp)
	AND oi._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'order_items'
	)

	AND oi._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'order_items'
	);

--order_payment table--
--Case1: payment_value < 0
INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'order_payments',
	_row_id,
	'negative_payment_value',
	'payment_value'
FROM staging.order_payments op
WHERE staging.try_parse_numeric(payment_value) < 0
	AND op._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'order_payments'
	)

	AND op._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'order_payments'
	);

--Case2: payment_sequential <= 0
INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'order_items',
	_row_id,
	'invalid_payment_sequential',
	'payment_sequential'
FROM staging.order_payments op
WHERE staging.try_parse_numeric(payment_sequential) <= 0
	AND op._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'order_payments'
	)

	AND op._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'order_payments'
	);

--Case3: payemnt_installment <=0

INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'order_items',
	_row_id,
	'invalid_payment_installment',
	'payment_installments'
FROM staging.order_payments op
WHERE staging.try_parse_numeric(payment_installments) <= 0
	AND op._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'order_payments'
	)

	AND op._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'order_payments'
	);

--reviews table--

--Case 1: review_score out of range 1-5

INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT 
	'reviews',
	_row_id,
	'invalid_review_score_range',
	'review_score'
FROM staging.reviews r
WHERE  staging.try_parse_int(review_score) < 1 AND  staging.try_parse_int(review_score) > 5
	AND r._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'reviews'
	)
	AND r._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'reviews'
	);

--Case 2: Review created before order_purchase
INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT 
	'reviews',
	r._row_id,
	'review_created_before_purchase',
	'review_creation_date'
FROM staging.reviews r JOIN staging.orders o
ON r.order_id = o.order_id
WHERE r.review_creation_date < o.order_purchase_timestamp
	AND r._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'reviews'
	)
	AND r._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'reviews'
	);

--Case3: Review answer before review creation
INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT 
	'reviews',
	_row_id,
	'review_answer_before_creation',
	'review_answer_timestamp'
FROM staging.reviews r
WHERE review_answer_timestamp < review_creation_date
	AND r._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'reviews'
	)
	AND r._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'reviews'
	);
	
---Products table---

--Case 1: Missing product_category_name

INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'products',
	_row_id,
	'missing_product_category_name',
	'product_category_name'
FROM staging.products p
WHERE product_category_name IS NULL
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'products')
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'products'
	); 

--Case 2: Missing product_photos_qty

INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'products',
	_row_id,
	'missing_product_photos_qty',
	'product_photos_qty'
FROM staging.products p
WHERE product_photos_qty IS NULL
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'products')
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'products'
	); 

--Case 3: Missing product_name_lenght

INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'products',
	_row_id,
	'missing_product_name_lenght',
	'product_name_lenght'
FROM staging.products p
WHERE product_name_lenght IS NULL
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'products')
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'products'
	); 

--Case 4: Missing product_description_lenght

INSERT INTO silver.business_validation_errors_log(
table_name,
row_id,
error_description,
failed_mismatched_column
)
SELECT
	'products',
	_row_id,
	'missing_product_description_lenght',
	'product_description_lenght'
FROM staging.products p
WHERE product_description_lenght IS NULL
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.datatype_validation_errors_log
		WHERE table_name = 'products')
	AND p._row_id NOT IN (
		SELECT row_id
		FROM silver.missing_values_logs
		WHERE table_name = 'products'
	); 

-- Case 5: Invalid (<= 0) product_name_length
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'products',
    p._row_id,
    'invalid_product_name_length',
    'product_name_lenght'
FROM staging.products p
WHERE 
    staging.try_parse_int(product_name_lenght) <= 0

    -- Exclude datatype validation failures
    AND p._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'products'
    )

    -- Exclude missing mandatory values
    AND p._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'products'
    );

-- Case 6: Invalid (<= 0) product_description_lenght

INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'products',
    p._row_id,
    'invalid_product_description_length',
    'product_description_lenght'
FROM staging.products p
WHERE staging.try_parse_int(product_description_lenght) <= 0
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.datatype_validation_errors_log
      WHERE table_name = 'products'
  )
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.missing_values_logs
      WHERE table_name = 'products'
  );

-- Case 7: Invalid (<= 0) product_photos_qty
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'products',
    p._row_id,
    'invalid_product_photos_qty',
    'product_photos_qty'
FROM staging.products p
WHERE staging.try_parse_int(product_photos_qty) <= 0
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.datatype_validation_errors_log
      WHERE table_name = 'products'
  )
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.missing_values_logs
      WHERE table_name = 'products'
  );

-- Case 8: Invalid (<= 0) product_weight_g
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'products',
    p._row_id,
    'invalid_product_weight_g',
    'product_weight_g'
FROM staging.products p
WHERE staging.try_parse_int(product_weight_g) <= 0
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.datatype_validation_errors_log
      WHERE table_name = 'products'
  )
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.missing_values_logs
      WHERE table_name = 'products'
  );
-- Case 9: Invalid (<= 0) product_length_cm

INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'products',
    p._row_id,
    'invalid_product_length_cm',
    'product_length_cm'
FROM staging.products p
WHERE staging.try_parse_int(product_length_cm) <= 0
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.datatype_validation_errors_log
      WHERE table_name = 'products'
  )
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.missing_values_logs
      WHERE table_name = 'products'
  );
  


-- Case 10: Invalid (<= 0) product_height_cm
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'products',
    p._row_id,
    'invalid_product_height_cm',
    'product_height_cm'
FROM staging.products p
WHERE staging.try_parse_int(product_height_cm) <= 0
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.datatype_validation_errors_log
      WHERE table_name = 'products'
  )
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.missing_values_logs
      WHERE table_name = 'products'
  );


-- Case 10: Invalid (<= 0) product_width_cm

INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'products',
    p._row_id,
    'invalid_product_width_cm',
    'product_width_cm'
FROM staging.products p
WHERE staging.try_parse_int(product_width_cm) <= 0
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.datatype_validation_errors_log
      WHERE table_name = 'products'
  )
  AND p._row_id NOT IN (
      SELECT row_id FROM silver.missing_values_logs
      WHERE table_name = 'products'
  );

--Sellers table--

--Case 1: Invalid seller_state
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'sellers',
    s._row_id,
    'invalid_seller_state',
    'seller_state'
FROM staging.sellers s
WHERE seller_state NOT IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS',
    'MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC',
    'SP','SE','TO'
)
AND s._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log
    WHERE table_name = 'sellers'
)
AND s._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs
    WHERE table_name = 'sellers'
);

--Case2: Invalid seller_zip_code_prefix
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'sellers',
    s._row_id,
    'invalid_seller_zip_code_prefix',
    'seller_zip_code_prefix'
FROM staging.sellers s
WHERE length(seller_zip_code_prefix) < 4
AND s._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log
    WHERE table_name = 'sellers'
)
AND s._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs
    WHERE table_name = 'sellers'
);

--geolocation table--

--Case1: Invalid latitude

INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'geolocation',
    g._row_id,
    'invalid_latitude_range',
    'geolocation_lat'
FROM staging.geolocation g
WHERE
    staging.try_parse_numeric(geolocation_lat) NOT BETWEEN -90 AND 90
AND g._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log WHERE table_name = 'geolocation'
)
AND g._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs WHERE table_name = 'geolocation'
);

--Case2: Invalid longitude
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'geolocation',
    g._row_id,
    'invalid_longitude_range',
    'geolocation_lng'
FROM staging.geolocation g
WHERE
    staging.try_parse_numeric(geolocation_lng) NOT BETWEEN -180 AND 180
AND g._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log WHERE table_name = 'geolocation'
)
AND g._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs WHERE table_name = 'geolocation'
);

--Case3: Invalid zip prefix
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'geolocation',
    g._row_id,
    'invalid_zip_code_prefix',
    'geolocation_zip_code_prefix'
FROM staging.geolocation g
WHERE
    staging.try_parse_int(geolocation_zip_code_prefix) IS NULL
    OR length(geolocation_zip_code_prefix) NOT BETWEEN 4 AND 5
AND g._row_id NOT IN (
    SELECT row_id FROM silver.datatype_validation_errors_log WHERE table_name = 'geolocation'
)
AND g._row_id NOT IN (
    SELECT row_id FROM silver.missing_values_logs WHERE table_name = 'geolocation'
);

--Case4: Invalid state
INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'geolocation',
    g._row_id,
    'invalid_state_code',
    'geolocation_state'
FROM staging.geolocation g
WHERE
    geolocation_state IS NOT NULL
    AND geolocation_state NOT IN (
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
        'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
        'RS','RO','RR','SC','SP','SE','TO'
    )
    AND g._row_id NOT IN (
        SELECT row_id
        FROM silver.datatype_validation_errors_log
        WHERE table_name = 'geolocation'
    )
    AND g._row_id NOT IN (
        SELECT row_id
        FROM silver.missing_values_logs
        WHERE table_name = 'geolocation'
    );

--product_Category_name_translation table

--Case 1: Same category maps to multiple english names

INSERT INTO silver.business_validation_errors_log (
    table_name,
    row_id,
    error_description,
    failed_mismatched_column
)
SELECT
    'product_category_name_translation',
    p._row_id,
    'conflicting_category_translation',
    'product_category_name_english'
FROM staging.product_category_name_translation p
JOIN (
    SELECT
        product_category_name
    FROM staging.product_category_name_translation
    GROUP BY product_category_name
    HAVING COUNT(DISTINCT product_category_name_english) > 1
) conflicts
ON p.product_category_name = conflicts.product_category_name;
