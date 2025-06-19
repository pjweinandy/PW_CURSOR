-- =====================================================
-- ETL Test Execution Script
-- =====================================================
-- Copy and paste this script into your Snowflake SQL editor
-- Run each section sequentially

-- =====================================================
-- STEP 1: Quick Validation Tests
-- =====================================================
-- Run this first to validate basic setup

-- Test 1: Check if configuration table has data
SELECT 
    'Configuration Check' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status,
    COUNT(*) AS record_count
FROM CONFIG_ALL.STAGE_FILE_LOAD_TYPE;

-- Test 2: Check if mapping table exists and has structure
SELECT 
    'Mapping Table Check' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status,
    COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'CONFIG_ALL' 
AND TABLE_NAME = 'STAGE_FILE_TABLE_MAPPING_PW';

-- Test 3: Check if procedure exists
SELECT 
    'Procedure Check' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status,
    COUNT(*) AS procedure_count
FROM INFORMATION_SCHEMA.PROCEDURES 
WHERE PROCEDURE_SCHEMA = 'CLIENT_STAGE_T' 
AND PROCEDURE_NAME = 'LOAD_STAGE_FILES_BY_CALC_DT_PW';

-- =====================================================
-- STEP 2: Basic ETL Workflow Test
-- =====================================================
-- Test the main workflow with reload=True

SELECT 'Starting Basic ETL Test...' AS status;

CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    TRUE
);

SELECT 'Basic ETL Test completed!' AS status;

-- =====================================================
-- STEP 3: Check Results
-- =====================================================
-- Verify that files were loaded successfully

SELECT 
    'ETL Results Check' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status,
    COUNT(*) AS loaded_files,
    SUM(CASE WHEN RAW_LOADED = TRUE THEN 1 ELSE 0 END) AS raw_loaded,
    SUM(CASE WHEN HIST_LOADED = TRUE THEN 1 ELSE 0 END) AS hist_loaded
FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
WHERE CALC_DT = DATE'2025-04-30';

-- =====================================================
-- STEP 4: Reload Logic Test
-- =====================================================
-- Test that files are skipped when reload=False

SELECT 'Starting Reload Logic Test...' AS status;

-- Get initial count
SELECT COUNT(*) AS initial_count 
FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
WHERE CALC_DT = DATE'2025-04-30';

-- Run with reload=False
CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    FALSE
);

-- Get final count
SELECT COUNT(*) AS final_count 
FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
WHERE CALC_DT = DATE'2025-04-30';

SELECT 'Reload Logic Test completed!' AS status;

-- =====================================================
-- STEP 5: Data Quality Check
-- =====================================================
-- Check record counts for created tables

SELECT 
    BASE_NAME,
    RAW_TABLE,
    HIST_TABLE,
    RAW_LOADED,
    HIST_LOADED,
    LAST_LOAD_DTTM
FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
WHERE CALC_DT = DATE'2025-04-30'
ORDER BY LAST_LOAD_DTTM DESC;

-- =====================================================
-- STEP 6: Table Record Counts
-- =====================================================
-- Check actual data in tables (run these individually if tables exist)

-- Check if LP_CLIENT_TEST_HIST exists and has data
SELECT 
    'LP_CLIENT_TEST_HIST' AS table_name,
    COUNT(*) AS record_count
FROM CLIENT_STAGE_T.LP_CLIENT_TEST_HIST 
WHERE CALC_DT = DATE'2025-04-30';

-- Check if LP_ASSET_TEST_HIST exists and has data
SELECT 
    'LP_ASSET_TEST_HIST' AS table_name,
    COUNT(*) AS record_count
FROM CLIENT_STAGE_T.LP_ASSET_TEST_HIST 
WHERE CALC_DT = DATE'2025-04-30';

-- Check if WHEELS_VEHICLE_DATA_TEST_HIST exists and has data
SELECT 
    'WHEELS_VEHICLE_DATA_TEST_HIST' AS table_name,
    COUNT(*) AS record_count
FROM CLIENT_STAGE_T.WHEELS_VEHICLE_DATA_TEST_HIST 
WHERE CALC_DT = DATE'2025-04-30';

-- =====================================================
-- STEP 7: Error Handling Test
-- =====================================================
-- Test with invalid date

SELECT 'Starting Error Handling Test...' AS status;

CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-01-01',  -- Date with no files
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    TRUE
);

SELECT 'Error Handling Test completed!' AS status;

-- =====================================================
-- STEP 8: Performance Test
-- =====================================================
-- Monitor execution time

SELECT CURRENT_TIMESTAMP() AS start_time;

CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    TRUE
);

SELECT CURRENT_TIMESTAMP() AS end_time;

-- =====================================================
-- FINAL SUMMARY
-- =====================================================
-- Overall test results

SELECT 
    'ETL Workflow Test Summary' AS summary,
    COUNT(*) AS total_files_processed,
    SUM(CASE WHEN RAW_LOADED = TRUE AND HIST_LOADED = TRUE THEN 1 ELSE 0 END) AS successful_loads,
    SUM(CASE WHEN RAW_LOADED = FALSE OR HIST_LOADED = FALSE THEN 1 ELSE 0 END) AS failed_loads
FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
WHERE CALC_DT = DATE'2025-04-30';

SELECT 'All tests completed! Review results above.' AS final_status; 