-- =====================================================
-- POPULATE_CLIENT_INPUTS_COMBINED
-- =====================================================
-- This procedure populates the CLIENT_INPUTS_COMBINED table with data from
-- the joined LP tables, creating JSON structures for asset/client/lease data
-- and receivables data. This version dynamically handles new columns.
-- 
-- Author: AI Assistant
-- Date: 2025-01-27
-- Purpose: Transform joined LP data into structured JSON format with dynamic column handling

CREATE OR REPLACE PROCEDURE CLIENT_STAGE_T.POPULATE_CLIENT_INPUTS_COMBINED(
    "V_CALC_DT" DATE
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_result STRING;
    v_start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_end_time TIMESTAMP_NTZ;
    v_record_count NUMBER := 0;
    v_error_message STRING;
    
    -- Exception for error handling
    DATA_TRANSFORMATION_ERROR EXCEPTION (-20003, 'Error during data transformation');
    
BEGIN
    -- Clear existing data for the calculation date
    DELETE FROM CLIENT_STAGE_T.CLIENT_INPUTS_COMBINED 
    WHERE CALC_DT = :V_CALC_DT;
    
    -- Insert data with dynamic JSON transformation
    INSERT INTO CLIENT_STAGE_T.CLIENT_INPUTS_COMBINED (
        CALC_DT,
        ASSET_ID,
        FLEETCO_ID,
        INELIGIBLE_IND,
        ASSET_JSON,
        RECV_JSON,
        LOAD_TIMESTAMP,
        MAPPING_ID
    )
    WITH asset_data AS (
        -- Get the main asset/client/lease data (one record per asset)
        SELECT DISTINCT
            A.CALC_DT,
            A.ASSETNUMBER AS ASSET_ID,
            F.FLEETCO_ID AS FLEETCO_ID,  -- Fixed: using correct field name
            FALSE AS INELIGIBLE_IND,  -- Leave empty for now as requested
            -- Create ASSET_JSON from combined asset, client, and lease data using dynamic column handling
            OBJECT_CONSTRUCT_KEEP_NULL(
                -- Asset data (all columns dynamically included)
                'ASSET_DATA', OBJECT_CONSTRUCT_KEEP_NULL(*),
                -- Client data (all columns dynamically included)
                'CLIENT_DATA', OBJECT_CONSTRUCT_KEEP_NULL(C.*),
                -- Lease data (all columns dynamically included)
                'LEASE_DATA', OBJECT_CONSTRUCT_KEEP_NULL(L.*)
            ) AS ASSET_JSON,
            CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP,
            'Inputs_Combined_V1' AS MAPPING_ID
        FROM CLIENT_STAGE_T.LP_ASSET_TEST_HIST A 
        LEFT JOIN CLIENT_STAGE_T.LP_CLIENT_TEST_HIST C
            ON A.CALC_DT = C.CALC_DT
            AND A.CLIENTNUMBER = C.CLIENTID
        LEFT JOIN CLIENT_STAGE_T.LP_LEASE_TEST_HIST L 
            ON L.CALC_DT = A.CALC_DT
            AND L.ASSETNUMBER = A.ASSETNUMBER
            AND L.CLIENTNUMBER = A.CLIENTNUMBER
        INNER JOIN CONFIG_LKP.LKP_ETL_STAGE_FLEETCO_ID F
            ON A.LENDERCODE = F.FLEETCO_ID_INPUT
            AND A.CALC_DT >= F.START_DT
            AND A.CALC_DT <= F.END_DT
        WHERE A.CALC_DT = :V_CALC_DT
    ),
    receivables_data AS (
        -- Get all receivables records for each asset (dynamic column handling)
        SELECT 
            A.ASSETNUMBER AS ASSET_ID,
            ARRAY_AGG(
                OBJECT_CONSTRUCT_KEEP_NULL(*)
            ) AS RECV_JSON
        FROM CLIENT_STAGE_T.LP_ASSET_TEST_HIST A
        LEFT JOIN CLIENT_STAGE_T.LP_ASSETINVOICEOPENAR_TEST_HIST R
            ON A.CALC_DT = R.CALC_DT
            AND A.ASSETNUMBER = R.ASSETNUMBER
            AND A.CLIENTNUMBER = R.CLIENTNUMBER
        WHERE A.CALC_DT = :V_CALC_DT
        GROUP BY A.ASSETNUMBER
    )
    SELECT 
        ad.CALC_DT,
        ad.ASSET_ID,
        ad.FLEETCO_ID,
        ad.INELIGIBLE_IND,
        ad.ASSET_JSON,
        COALESCE(rd.RECV_JSON, ARRAY_CONSTRUCT()) AS RECV_JSON,
        ad.LOAD_TIMESTAMP,
        ad.MAPPING_ID
    FROM asset_data ad
    LEFT JOIN receivables_data rd ON ad.ASSET_ID = rd.ASSET_ID;
    
    -- Get record count
    SELECT COUNT(*) INTO :v_record_count 
    FROM CLIENT_STAGE_T.CLIENT_INPUTS_COMBINED 
    WHERE CALC_DT = :V_CALC_DT;
    
    v_end_time := CURRENT_TIMESTAMP();
    
    -- Return success message
    v_result := 'Successfully populated CLIENT_INPUTS_COMBINED with ' || 
                :v_record_count || ' records for CALC_DT ' || 
                TO_VARCHAR(:V_CALC_DT, 'YYYY-MM-DD') || 
                ' in ' || TIMESTAMPDIFF(SECOND, :v_start_time, :v_end_time) || ' seconds.';
    
    RETURN :v_result;
    
EXCEPTION
    WHEN OTHER THEN
        v_error_message := 'Error in POPULATE_CLIENT_INPUTS_COMBINED: ' || SQLERRM;
        RAISE DATA_TRANSFORMATION_ERROR;
END;
$$;

-- =====================================================
-- USAGE EXAMPLE
-- =====================================================
/*
-- Execute the procedure
CALL CLIENT_STAGE_T.POPULATE_CLIENT_INPUTS_COMBINED(DATE'2025-04-30');

-- Verify results
SELECT 
    CALC_DT,
    ASSET_ID,
    FLEETCO_ID,
    INELIGIBLE_IND,
    ARRAY_SIZE(RECV_JSON) AS receivables_count,
    LOAD_TIMESTAMP,
    MAPPING_ID
FROM CLIENT_STAGE_T.CLIENT_INPUTS_COMBINED 
WHERE CALC_DT = DATE'2025-04-30'
ORDER BY ASSET_ID;

-- Sample JSON structure (now with dynamic columns)
SELECT 
    ASSET_ID,
    ASSET_JSON:ASSET_DATA:ASSETNUMBER AS asset_number,
    ASSET_JSON:CLIENT_DATA:CLIENTNAME AS client_name,
    ASSET_JSON:LEASE_DATA:CONTRACTNUMBER AS contract_number,
    RECV_JSON
FROM CLIENT_STAGE_T.CLIENT_INPUTS_COMBINED 
WHERE CALC_DT = DATE'2025-04-30'
LIMIT 5;

-- Check for new columns (example query to see what columns are available)
SELECT 
    'ASSET_DATA' AS data_section,
    OBJECT_KEYS(ASSET_JSON:ASSET_DATA) AS available_columns
FROM CLIENT_STAGE_T.CLIENT_INPUTS_COMBINED 
WHERE CALC_DT = DATE'2025-04-30'
LIMIT 1;
*/ 