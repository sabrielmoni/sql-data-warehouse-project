/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the "COPY" command to load data from csv files to bronze tables.

Parameters: none. This SP does not accept any parameters or return any values.

Usage example:
    CALL load_bronze();
*/

DROP FUNCTION IF EXISTS load_bronze;

CREATE OR REPLACE PROCEDURE load_bronze()
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
    RAISE NOTICE 'Starting data load for Bronze Layer at %', total_start_time;

    -- crm_cust_info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: crm_cust_info at %', start_time;
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE 'Loading data into crm_cust_info...';
        COPY bronze.crm_cust_info 
        FROM '/Users/sabri/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',');

        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'crm_cust_info loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading crm_cust_info: %', SQLERRM;
    END;

    -- crm_prd_info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: crm_prd_info at %', start_time;
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE 'Loading data into crm_prd_info...';
        COPY bronze.crm_prd_info 
        FROM '/Users/sabri/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',');

        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'crm_prd_info loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading crm_prd_info: %', SQLERRM;
    END;

    -- crm_sales_details
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: crm_sales_details at %', start_time;
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE 'Loading data into crm_sales_details...';
        COPY bronze.crm_sales_details 
        FROM '/Users/sabri/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',');

        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'crm_sales_details loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading crm_sales_details: %', SQLERRM;
    END;

    -- erp_cust_az12
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: erp_cust_az12 at %', start_time;
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE 'Loading data into erp_cust_az12...';
        COPY bronze.erp_cust_az12 
        FROM '/Users/sabri/sql-data-warehouse-project/datasets/source_crm/CUST_AZ12.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',');

        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'erp_cust_az12 loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    -- erp_loc_a101
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: erp_loc_a101 at %', start_time;
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE 'Loading data into erp_loc_a101...';
        COPY bronze.erp_loc_a101 
        FROM '/Users/sabri/sql-data-warehouse-project/datasets/source_crm/LOC_A101.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',');

        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'erp_loc_a101 loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading erp_loc_a101: %', SQLERRM;
    END;

    -- erp_px_cat_g1v2
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating table: erp_px_cat_g1v2 at %', start_time;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE 'Loading data into erp_px_cat_g1v2...';
        COPY bronze.erp_px_cat_g1v2 
        FROM '/Users/sabri/sql-data-warehouse-project/datasets/source_crm/PX_CAT_G1V2.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',');

        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'erp_px_cat_g1v2 loaded successfully! Duration: % seconds', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading erp_px_cat_g1v2: %', SQLERRM;
    END;

    -- Guardar el tiempo de finalización total
    total_end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (total_end_time - total_start_time));
    RAISE NOTICE 'Bronze Layer load completed successfully at % (Total Duration: % seconds)', total_end_time, duration;
END;
$$;
