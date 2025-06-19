# ETL Workflow Testing Checklist

## Pre-Testing Setup ✅

### Environment Validation
- [ ] Snowflake connection established
- [ ] Required schemas exist (`CLIENT_STAGE_T`, `CONFIG_ALL`, `TESTING`)
- [ ] Source files uploaded to stage `@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO`
- [ ] Configuration table `CONFIG_ALL.STAGE_FILE_LOAD_TYPE` populated
- [ ] All stored procedures deployed

### Data Preparation
- [ ] Source files reduced to 100 records each (completed)
- [ ] Files named with correct date suffix (e.g., `_20250430`)
- [ ] Files placed in correct stage path: `2025_04/2025_04_30/`

## Phase 1: Quick Validation Tests 🚀

### Run Quick Test Runner
```sql
-- Execute the quick validation script
-- File: TESTING/QUICK_TEST_RUNNER.sql
```

**Expected Results:**
- [ ] Configuration Check: PASS (should have records)
- [ ] Mapping Table Check: PASS (should have columns)
- [ ] Procedure Check: PASS (procedure should exist)
- [ ] Stage Access Check: INFO (verify stage exists)

## Phase 2: Basic Functionality Tests 🔧

### Test 1: Initial Load
```sql
-- Test with reload=True
CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    TRUE
);
```

**Validation Points:**
- [ ] No errors during execution
- [ ] Returns success message
- [ ] Check mapping table for new entries
- [ ] Verify RAW tables created
- [ ] Verify HIST tables created/populated

### Test 2: Reload Logic
```sql
-- Test with reload=False (should skip existing files)
CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    FALSE
);
```

**Validation Points:**
- [ ] No new entries in mapping table
- [ ] Execution completes quickly
- [ ] No duplicate data created

## Phase 3: Data Quality Tests 📊

### Test 3: Record Count Validation
```sql
-- Check record counts for each table
SELECT 
    BASE_NAME,
    RAW_TABLE,
    HIST_TABLE,
    (SELECT COUNT(*) FROM CLIENT_STAGE_T.RAW_TABLE) AS raw_count,
    (SELECT COUNT(*) FROM CLIENT_STAGE_T.HIST_TABLE WHERE CALC_DT = DATE'2025-04-30') AS hist_count
FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW 
WHERE CALC_DT = DATE'2025-04-30';
```

**Validation Points:**
- [ ] RAW table count = 100 (or expected count)
- [ ] HIST table count = RAW table count
- [ ] No empty tables
- [ ] CALC_DT properly set in HIST tables

### Test 4: Data Integrity
```sql
-- Sample data validation for each table
SELECT 'LP_CLIENT_TEST' AS table_name, COUNT(*) AS total_records, COUNT(DISTINCT ClientKey) AS unique_keys
FROM CLIENT_STAGE_T.LP_CLIENT_TEST_HIST 
WHERE CALC_DT = DATE'2025-04-30'
UNION ALL
SELECT 'LP_ASSET_TEST' AS table_name, COUNT(*) AS total_records, COUNT(DISTINCT AssetKey) AS unique_keys
FROM CLIENT_STAGE_T.LP_ASSET_TEST_HIST 
WHERE CALC_DT = DATE'2025-04-30';
```

**Validation Points:**
- [ ] No duplicate primary keys
- [ ] All required columns populated
- [ ] Data types correct
- [ ] No NULL values in key fields

## Phase 4: Error Handling Tests ⚠️

### Test 5: Invalid Date Handling
```sql
-- Test with date that has no files
CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-01-01',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    TRUE
);
```

**Validation Points:**
- [ ] No errors thrown
- [ ] Graceful handling of empty stage
- [ ] Appropriate return message

### Test 6: Invalid Stage Handling
```sql
-- Test with non-existent stage (if possible)
-- This may require creating a test scenario
```

## Phase 5: Performance Tests ⚡

### Test 7: Execution Time
```sql
-- Monitor execution time
SELECT CURRENT_TIMESTAMP() AS start_time;

CALL CLIENT_STAGE_T.LOAD_STAGE_FILES_BY_CALC_DT_PW(
    DATE'2025-04-30',
    '@CLIENT_STAGE_T.INTERNAL_ETL_STAGE_DEMO',
    TRUE
);

SELECT CURRENT_TIMESTAMP() AS end_time;
```

**Validation Points:**
- [ ] Execution time < 5 minutes for 100-record files
- [ ] No timeout errors
- [ ] Consistent performance

## Phase 6: Comprehensive Test Suite 🧪

### Run Full Test Suite
```sql
-- Execute comprehensive test suite
CALL TESTING.RUN_ALL_ETL_TESTS();

-- View results
SELECT * FROM TESTING.ETL_TEST_SUMMARY;
```

**Expected Results:**
- [ ] All tests PASS
- [ ] No critical errors
- [ ] Performance within acceptable limits

## Phase 7: Production Readiness Tests 🚀

### Test 8: Schema Evolution
```sql
-- Test adding new columns to source files
-- This would require modifying source files and testing DDL adjustments
```

### Test 9: Delta Processing
```sql
-- Test delta processing for tables with DELTA_IND = TRUE
-- Verify TEMPLATE_DELTA_SQL execution
```

### Test 10: Large File Handling
```sql
-- Test with larger files (if needed)
-- Monitor memory usage and performance
```

## Post-Testing Cleanup 🧹

### Data Cleanup
- [ ] Remove test data if needed
- [ ] Reset mapping table entries for test date
- [ ] Drop test tables if created

### Documentation
- [ ] Record test results
- [ ] Document any issues found
- [ ] Update configuration if needed
- [ ] Create runbook for production deployment

## Success Criteria ✅

### All tests must pass:
- [ ] Basic functionality works
- [ ] Reload logic works correctly
- [ ] Data quality maintained
- [ ] Error handling graceful
- [ ] Performance acceptable
- [ ] No data loss or corruption

### Production Readiness:
- [ ] All procedures tested
- [ ] Configuration validated
- [ ] Error scenarios covered
- [ ] Performance baseline established
- [ ] Documentation complete

---

## Quick Reference Commands

```sql
-- Check current status
SELECT * FROM CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW WHERE CALC_DT = DATE'2025-04-30';

-- View test results
SELECT * FROM TESTING.ETL_TEST_SUMMARY;

-- Check table counts
SELECT 
    TABLE_NAME, 
    ROW_COUNT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'CLIENT_STAGE_T' 
AND TABLE_NAME LIKE '%_HIST';
``` 