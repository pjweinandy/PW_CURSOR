-- =====================================================
-- ETL Workflow Testing Suite
-- =====================================================
-- This script provides comprehensive testing for the ETL workflow
-- Author: AI Assistant
-- Date: 2025-01-27
-- Purpose: Test LOAD_STAGE_FILES_BY_CALC_DT_PW workflow

-- =====================================================
-- TEST 1: Basic Functionality Test
-- =====================================================
-- Test the main workflow with a single file
-- Expected: File loads successfully to RAW and HIST tables

-- Setup test data
CREATE OR REPLACE TABLE TESTING.ETL_TEST_RESULTS (
    test_name STRING,
    test_description STRING,
    expected_result STRING,
    actual_result STRING,
    test_status STRING,
    execution_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    error_message STRING
);

-- Test 1: Basic ETL Workflow
CREATE OR REPLACE PROCEDURE TESTING.TEST_BASIC_ETL_WORKFLOW()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_result STRING;
    v_start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_end_time TIMESTAMP_NTZ;
    v_test_status STRING;
    v_error_message STRING;
BEGIN
    -- Test the main ETL workflow
    CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
        DATE'2025-04-30',
        '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
        TRUE
    ) INTO :v_result;
    
    v_end_time := CURRENT_TIMESTAMP();
    
    -- Check if tables were created and populated
    IF (EXISTS (
        SELECT 1 FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
        WHERE CALC_DT = DATE'2025-04-30' 
        AND RAW_LOADED = TRUE 
        AND HIST_LOADED = TRUE
    )) THEN
        v_test_status := 'PASS';
        v_error_message := NULL;
    ELSE
        v_test_status := 'FAIL';
        v_error_message := 'Tables not properly loaded';
    END IF;
    
    -- Log test results
    INSERT INTO TESTING.ETL_TEST_RESULTS (
        test_name, test_description, expected_result, actual_result, 
        test_status, error_message
    ) VALUES (
        'Basic ETL Workflow',
        'Test main ETL workflow with reload=True',
        'Files loaded to RAW and HIST tables',
        :v_result,
        :v_test_status,
        :v_error_message
    );
    
    RETURN 'Test completed: ' || v_test_status;
END;
$$;

-- =====================================================
-- TEST 2: Reload Logic Test
-- =====================================================
-- Test that files are skipped when already loaded and reload=False

CREATE OR REPLACE PROCEDURE TESTING.TEST_RELOAD_LOGIC()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_result STRING;
    v_test_status STRING;
    v_error_message STRING;
    v_initial_count NUMBER;
    v_final_count NUMBER;
BEGIN
    -- Get initial count of loaded files
    SELECT COUNT(*) INTO :v_initial_count 
    FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
    WHERE CALC_DT = DATE'2025-04-30';
    
    -- Run ETL with reload=False (should skip existing files)
    CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
        DATE'2025-04-30',
        '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
        FALSE
    ) INTO :v_result;
    
    -- Get final count
    SELECT COUNT(*) INTO :v_final_count 
    FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
    WHERE CALC_DT = DATE'2025-04-30';
    
    -- Test should not create new entries
    IF (:v_final_count = :v_initial_count) THEN
        v_test_status := 'PASS';
        v_error_message := NULL;
    ELSE
        v_test_status := 'FAIL';
        v_error_message := 'Files were reloaded when they should have been skipped';
    END IF;
    
    -- Log test results
    INSERT INTO TESTING.ETL_TEST_RESULTS (
        test_name, test_description, expected_result, actual_result, 
        test_status, error_message
    ) VALUES (
        'Reload Logic Test',
        'Test that files are skipped when reload=False',
        'No new files loaded (count unchanged)',
        'Initial: ' || :v_initial_count || ', Final: ' || :v_final_count,
        :v_test_status,
        :v_error_message
    );
    
    RETURN 'Test completed: ' || v_test_status;
END;
$$;

-- =====================================================
-- TEST 3: Data Quality Test
-- =====================================================
-- Test that data integrity is maintained through the ETL process

CREATE OR REPLACE PROCEDURE TESTING.TEST_DATA_QUALITY()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_test_status STRING := 'PASS';
    v_error_message STRING := '';
    v_raw_count NUMBER;
    v_hist_count NUMBER;
    v_base_name STRING;
    v_raw_table STRING;
    v_hist_table STRING;
    v_sql STRING;
BEGIN
    -- Test each loaded table for data quality
    FOR table_rec IN (
        SELECT BASE_NAME, RAW_TABLE, HIST_TABLE 
        FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
        WHERE CALC_DT = DATE'2025-04-30' 
        AND RAW_LOADED = TRUE 
        AND HIST_LOADED = TRUE
    ) DO
        v_base_name := table_rec.BASE_NAME;
        v_raw_table := table_rec.RAW_TABLE;
        v_hist_table := table_rec.HIST_TABLE;
        
        -- Check RAW table count
        v_sql := 'SELECT COUNT(*) FROM CLIENT_STAGE_T.' || :v_raw_table;
        EXECUTE IMMEDIATE :v_sql INTO :v_raw_count;
        
        -- Check HIST table count
        v_sql := 'SELECT COUNT(*) FROM CLIENT_STAGE_T.' || :v_hist_table || ' WHERE CALC_DT = DATE''2025-04-30''';
        EXECUTE IMMEDIATE :v_sql INTO :v_hist_count;
        
        -- Validate counts
        IF (:v_raw_count = 0) THEN
            v_test_status := 'FAIL';
            v_error_message := v_error_message || 'RAW table ' || :v_raw_table || ' is empty; ';
        END IF;
        
        IF (:v_hist_count = 0) THEN
            v_test_status := 'FAIL';
            v_error_message := v_error_message || 'HIST table ' || :v_hist_table || ' has no records for CALC_DT; ';
        END IF;
        
        -- Check for data consistency (RAW and HIST should have same count for this date)
        IF (:v_raw_count != :v_hist_count) THEN
            v_test_status := 'FAIL';
            v_error_message := v_error_message || 'Count mismatch for ' || :v_base_name || ' (RAW: ' || :v_raw_count || ', HIST: ' || :v_hist_count || '); ';
        END IF;
    END FOR;
    
    -- Log test results
    INSERT INTO TESTING.ETL_TEST_RESULTS (
        test_name, test_description, expected_result, actual_result, 
        test_status, error_message
    ) VALUES (
        'Data Quality Test',
        'Test data integrity and record counts',
        'All tables have data and counts match',
        'Data quality validation completed',
        :v_test_status,
        CASE WHEN :v_error_message = '' THEN NULL ELSE :v_error_message END
    );
    
    RETURN 'Test completed: ' || v_test_status;
END;
$$;

-- =====================================================
-- TEST 4: Error Handling Test
-- =====================================================
-- Test how the system handles invalid files or missing data

CREATE OR REPLACE PROCEDURE TESTING.TEST_ERROR_HANDLING()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_result STRING;
    v_test_status STRING;
    v_error_message STRING;
BEGIN
    -- Test with invalid date (should handle gracefully)
    BEGIN
        CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
            DATE'2025-01-01',  -- Date with no files
            '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
            TRUE
        ) INTO :v_result;
        
        v_test_status := 'PASS';
        v_error_message := 'System handled empty stage gracefully';
    EXCEPTION
        WHEN OTHER THEN
            v_test_status := 'FAIL';
            v_error_message := 'System failed to handle empty stage: ' || SQLERRM;
    END;
    
    -- Log test results
    INSERT INTO TESTING.ETL_TEST_RESULTS (
        test_name, test_description, expected_result, actual_result, 
        test_status, error_message
    ) VALUES (
        'Error Handling Test',
        'Test system behavior with invalid/missing data',
        'System handles errors gracefully',
        :v_result,
        :v_test_status,
        :v_error_message
    );
    
    RETURN 'Test completed: ' || v_test_status;
END;
$$;

-- =====================================================
-- TEST 5: Performance Test
-- =====================================================
-- Test execution time and performance metrics

CREATE OR REPLACE PROCEDURE TESTING.TEST_PERFORMANCE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_execution_time NUMBER;
    v_test_status STRING;
    v_error_message STRING;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();
    
    -- Run ETL process
    CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
        DATE'2025-04-30',
        '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
        TRUE
    );
    
    v_end_time := CURRENT_TIMESTAMP();
    v_execution_time := TIMESTAMPDIFF(SECOND, :v_start_time, :v_end_time);
    
    -- Performance threshold: should complete within 300 seconds (5 minutes)
    IF (:v_execution_time < 300) THEN
        v_test_status := 'PASS';
        v_error_message := NULL;
    ELSE
        v_test_status := 'FAIL';
        v_error_message := 'Execution time exceeded threshold: ' || :v_execution_time || ' seconds';
    END IF;
    
    -- Log test results
    INSERT INTO TESTING.ETL_TEST_RESULTS (
        test_name, test_description, expected_result, actual_result, 
        test_status, error_message
    ) VALUES (
        'Performance Test',
        'Test ETL execution time',
        'Execution time < 300 seconds',
        'Execution time: ' || :v_execution_time || ' seconds',
        :v_test_status,
        :v_error_message
    );
    
    RETURN 'Test completed: ' || v_test_status || ' (Time: ' || :v_execution_time || 's)';
END;
$$;

-- =====================================================
-- TEST EXECUTION SCRIPT
-- =====================================================
-- Run all tests in sequence

CREATE OR REPLACE PROCEDURE TESTING.RUN_ALL_ETL_TESTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_result STRING;
    v_final_result STRING := '';
BEGIN
    -- Clear previous test results
    DELETE FROM TESTING.ETL_TEST_RESULTS WHERE test_name LIKE '%ETL%';
    
    -- Run all tests
    CALL TESTING.TEST_BASIC_ETL_WORKFLOW() INTO :v_result;
    v_final_result := v_final_result || 'Basic Test: ' || :v_result || '\n';
    
    CALL TESTING.TEST_RELOAD_LOGIC() INTO :v_result;
    v_final_result := v_final_result || 'Reload Test: ' || :v_result || '\n';
    
    CALL TESTING.TEST_DATA_QUALITY() INTO :v_result;
    v_final_result := v_final_result || 'Quality Test: ' || :v_result || '\n';
    
    CALL TESTING.TEST_ERROR_HANDLING() INTO :v_result;
    v_final_result := v_final_result || 'Error Test: ' || :v_result || '\n';
    
    CALL TESTING.TEST_PERFORMANCE() INTO :v_result;
    v_final_result := v_final_result || 'Performance Test: ' || :v_result || '\n';
    
    -- Return summary
    RETURN v_final_result;
END;
$$;

-- =====================================================
-- TEST RESULTS QUERY
-- =====================================================
-- Query to view test results

CREATE OR REPLACE VIEW TESTING.ETL_TEST_SUMMARY AS
SELECT 
    test_name,
    test_status,
    execution_time,
    CASE 
        WHEN test_status = 'PASS' THEN '✅'
        WHEN test_status = 'FAIL' THEN '❌'
        ELSE '⚠️'
    END AS status_icon,
    error_message
FROM TESTING.ETL_TEST_RESULTS 
WHERE test_name LIKE '%ETL%'
ORDER BY execution_time DESC;

-- =====================================================
-- USAGE INSTRUCTIONS
-- =====================================================
/*
To run all tests:
CALL TESTING.RUN_ALL_ETL_TESTS();

To run individual tests:
CALL TESTING.TEST_BASIC_ETL_WORKFLOW();
CALL TESTING.TEST_RELOAD_LOGIC();
CALL TESTING.TEST_DATA_QUALITY();
CALL TESTING.TEST_ERROR_HANDLING();
CALL TESTING.TEST_PERFORMANCE();

To view results:
SELECT * FROM TESTING.ETL_TEST_SUMMARY;
*/ 