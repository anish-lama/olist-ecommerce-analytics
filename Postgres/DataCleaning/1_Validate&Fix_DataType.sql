TRUNCATE TABLE silver.datatype_validation_errors_log;
TRUNCATE TABLE silver.missing_values_logs;
TRUNCATE TABLE silver.business_validation_errors_log;


-- 1) Validate and fix Data Types


CALL staging.validate_datatypes();

SELECT * FROM silver.datatype_validation_errors_log;