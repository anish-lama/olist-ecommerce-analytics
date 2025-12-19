--------Timestamp Validator----------

CREATE OR REPLACE FUNCTION staging.try_parse_timestamp(value TEXT)
RETURNS TIMESTAMP
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN value::TIMESTAMP;
EXCEPTION WHEN OTHERS THEN
	RETURN NULL;
END;
$$;
-------INT Validator------

CREATE OR REPLACE FUNCTION staging.try_parse_int(value TEXT)
RETURNS INT
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN value::INT;
EXCEPTION WHEN OTHERS THEN
	RETURN NULL;
END;
$$;

-------NUMERIC Validator------
CREATE OR REPLACE FUNCTION staging.try_parse_numeric(value TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN value::NUMERIC;
EXCEPTION WHEN OTHERS THEN
	RETURN NULL;
END;
$$;

--------Datatype validation engine--------
CREATE OR REPLACE PROCEDURE staging.validate_datatypes()
LANGUAGE plpgsql
AS $$
DECLARE
	r RECORD;
	dynamic_sql TEXT;
	invalid_count INT;
BEGIN
	RAISE NOTICE 'Staring dataype validation....';

	FOR r IN
		SELECT table_name, column_name, expected_type
		FROM staging.expected_column_types
		ORDER BY table_name, column_name
	LOOP
		dynamic_sql := NULL;

		RAISE NOTICE 'Validating %.% expecting %', r.table_name, r.column_name, r.expected_type;

		---- Indentify invalid values based on expected datatype----
		IF r.expected_type = 'INT' THEN
			dynamic_sql := format($sql$
				INSERT INTO silver.datatype_validation_errors_log
				(table_name, column_name, row_id, invalid_value, expected_type, validation_rule, row_data)
				SELECT
					%L, %L, _row_id, %I::TEXT, %L, 'not_integer', to_jsonb(t)
				FROM staging.%I t
				WHERE %I IS NOT NULL
				AND staging.try_parse_int(%I) IS NULL;
			$sql$, r.table_name, r.column_name, r.column_name, r.expected_type, r.table_name, r.column_name, r.column_name);
		
		ELSIF r.expected_type = 'NUMERIC' THEN
			dynamic_sql := format($sql$
				INSERT INTO silver.datatype_validation_errors_log
				(table_name, column_name, row_id, invalid_value, expected_type, validation_rule, row_data)
				SELECT
					%L, %L, _row_id, %I::TEXT, %L, 'not_integer', to_jsonb(t)
				FROM staging.%I t
				WHERE %I IS NOT NULL
				AND staging.try_parse_numeric(%I) IS NULL;
			$sql$, r.table_name, r.column_name, r.column_name, r.expected_type, r.table_name, r.column_name, r.column_name);
		
		ELSIF r.expected_type = 'TIMESTAMP' THEN
			dynamic_sql := format($sql$
				INSERT INTO silver.datatype_validation_errors_log
				(table_name, column_name, row_id, invalid_value, expected_type, validation_rule, row_data)
				SELECT
					%L, %L, _row_id, %I::TEXT, %L, 'not_integer', to_jsonb(t)
				FROM staging.%I t
				WHERE %I IS NOT NULL
				AND staging.try_parse_timestamp(%I) IS NULL;
			$sql$, r.table_name, r.column_name, r.column_name, r.expected_type, r.table_name, r.column_name, r.column_name, r.column_name);	
		
		ELSIF r.expected_type = 'TEXT' THEN
			CONTINUE;
		
		ELSE
			RAISE WARNING 'Unknown dataype %, skipping...', r.expected_type;
			CONTINUE;
		END IF;

		IF dynamic_sql IS NOT NULL THEN
			EXECUTE dynamic_sql;
		END IF;

		---------NULLIFY the invalid column---------
		IF r.expected_type IN ('INT', 'NUMERIC', 'TIMESTAMP') THEN

			dynamic_sql := format($sql$
				UPDATE staging.%I
				SET %I = NULL
				WHERE _row_id IN (
					SELECT row_id
					FROM silver.datatype_validation_errors_log
					WHERE table_name = %L AND column_name = %L
				);
			$sql$, r.table_name, r.column_name, r.table_name, r.column_name);

			EXECUTE dynamic_sql;
		END IF;
	END LOOP;

		RAISE NOTICE 'Datatype validation completed';
END;
$$;
		