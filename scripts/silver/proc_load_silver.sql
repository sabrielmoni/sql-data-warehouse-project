/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from the 'bronze' schema.
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Inserts data from bronze tables into corresponding silver tables.

Parameters: none. This SP does not accept any parameters or return any values.

Usage example:
    CALL load_silver();
*/

DROP FUNCTION IF EXISTS load_silver;

CREATE OR REPLACE PROCEDURE load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    total_start_time TIMESTAMP;
    total_end_time TIMESTAMP;
    duration NUMERIC;
BEGIN
	-- Guardar el tiempo de inicio total
    total_start_time := clock_timestamp();
    RAISE NOTICE 'Starting data load for Silver Layer at %', total_start_time;

    -- crm_cust_info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: crm_cust_info at %', start_time;
        TRUNCATE TABLE silver.crm_cust_info;
        RAISE NOTICE 'Loading data into silver.crm_cust_info...';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info)
		WHERE flag_last = 1;
		
		end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'silver.crm_cust_info loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.crm_cust_info: %', SQLERRM;
    END;

    -- crm_prd_info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: crm_prd_info at %', start_time;
        TRUNCATE TABLE silver.crm_prd_info;

        RAISE NOTICE 'Loading data into silver.crm_prd_info...';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			prd_key,
			cat_id,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key FROM 7 FOR LENGTH(prd_key)) AS prd_key, -- Extract product key
			prd_nm,
			COALESCE(prd_cost, 0) as prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST (prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1
			AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
		
		end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'silver.crm_prd_info loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.crm_prd_info: %', SQLERRM;
    END;
	
	-- crm_sales_info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: crm_sales_info at %', start_time;
        TRUNCATE TABLE silver.crm_sales_info;

        RAISE NOTICE 'Loading data into silver.crm_sales_info...';
		INSERT INTO silver.crm_sales_info (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
				ELSE CAST(sls_order_dt::text AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
				ELSE CAST(sls_ship_dt::text AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
				ELSE CAST(sls_due_dt::text AS DATE)
			END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_info;
		
		end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'silver.crm_sales_details loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.crm_sales_details: %', SQLERRM;
    END;
	
	-- erp_cust_az12
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: erp_cust_az12 at %', start_time;
        TRUNCATE TABLE silver.erp_cust_az12;
        RAISE NOTICE 'Loading data into silver.erp_cust_az12...';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate, 
			gen)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid, -- Remove 'NAS' prefix if present
		CASE WHEN bdate > CURRENT_DATE THEN NULL
			ELSE bdate
		END AS bdate, -- Set future birthdates to NULL
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
		
		end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'silver.erp_cust_az12 loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.erp_cust_az12: %', SQLERRM;
    END;

	-- erp_loc_a101
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: erp_loc_a101 at %', start_time;
        TRUNCATE TABLE silver.erp_loc_a101;
        RAISE NOTICE 'Loading data into silver.erp_loc_a101...';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			REPLACE(cid, '-', '') cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') IN
		(SELECT cst_key FROM silver.crm_cust_info);
		
		end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'silver.erp_loc_a101 loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.erp_loc_a101: %', SQLERRM;
    END;
	
	-- erp_px_cat_g1v2
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: erp_px_cat_g1v2 at %', start_time;
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        RAISE NOTICE 'Loading data into silver.erp_px_cat_g1v2...';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id, 
			cat, 
			subcat, 
			maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;

		end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'silver.erp_px_cat_g1v2 loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.erp_px_cat_g1v2: %', SQLERRM;
    END;

    -- Guardar el tiempo de finalización total
    total_end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (total_end_time - total_start_time));
    RAISE NOTICE 'Silver Layer load completed successfully at % (Total Duration: % seconds)', total_end_time, duration;
END;
$$;
