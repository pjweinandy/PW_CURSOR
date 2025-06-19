-- =====================================================
-- Quick ETL Test Runner
-- =====================================================
-- Simple test to validate ETL workflow functionality
-- Run this before the full test suite for quick validation

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

-- Test 3: Check if source files are accessible (simulate stage listing)
SELECT 
    'Stage Access Check' AS test_name,
    'INFO' AS status,
    'Verify stage @CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO exists and is accessible' AS message;

-- Test 4: Validate procedure exists
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

-- Test 5: Check recent ETL activity (if any)
SELECT 
    'Recent ETL Activity' AS test_name,
    CASE 
        WHEN COUNT(*) > 0 THEN 'INFO' 
        ELSE 'INFO' 
    END AS status,
    COUNT(*) AS recent_loads,
    MAX(LAST_LOAD_DTTM) AS last_load_time
FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
WHERE LAST_LOAD_DTTM >= DATEADD(day, -7, CURRENT_TIMESTAMP());

-- =====================================================
-- Manual Test Commands
-- =====================================================
/*
-- Run these commands manually to test the ETL workflow:

-- 1. Test with reload=True (will process all files)
CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    TRUE
);

-- 2. Check results
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

-- 3. Test with reload=False (should skip existing files)
CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    FALSE
);

-- 4. Check data in created tables
SELECT 
    'LP_CLIENT_TEST' AS table_name,
    COUNT(*) AS record_count
FROM CLIENT_STAGE_T.LP_CLIENT_TEST_HIST 
WHERE CALC_DT = DATE'2025-04-30'
UNION ALL
SELECT 
    'LP_ASSET_TEST' AS table_name,
    COUNT(*) AS record_count
FROM CLIENT_STAGE_T.LP_ASSET_TEST_HIST 
WHERE CALC_DT = DATE'2025-04-30';
*/ 