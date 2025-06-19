CREATE OR REPLACE PROCEDURE CLIENT_STAGE_T.LOAD_STAGE_FILES_INTO_RAW("V_STAGE_PATH_WITH_FILE" VARCHAR, "V_BASE_NAME" VARCHAR, "V_CALC_DT" DATE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    --constants; put into etl variables
    V_SCHEMA_NAME STRING := CURRENT_SCHEMA();
    V_FILE_FORMAT STRING := ''CLIENT_STAGE_T.ETL_PIPE_CSV'';
    V_RAW_SUFFIX := ''_RAW'';
    V_HIST_SUFFIX := ''_HIST'';
    
    V_STAGE_PATH STRING := REGEXP_REPLACE(:V_STAGE_PATH_WITH_FILE, ''/[^/]*$'', '''');
    FILE_NAMES STRING;
    V_SQL STRING;
    V_INFER_SCHEMA_VALID_CHECK BOOLEAN;
    V_DT STRING := (SELECT TO_CHAR(:V_CALC_DT,''YYYYMMDD''));
    V_RAW_TABLE STRING;
    V_RAW_TABLE_NAME_FULL STRING;
    V_HIST_TABLE STRING;
    V_HIST_TABLE_NAME_FULL STRING;

    V_COLUMN_NAME STRING;
    V_DATA_TYPE STRING;
    V_CURRENT_TS TIMESTAMP := CURRENT_TIMESTAMP();
    V_DYNAMIC_RAW BOOLEAN;

BEGIN

    V_RAW_TABLE := :V_BASE_NAME || ''_'' || :V_DT || :V_RAW_SUFFIX;
    V_RAW_TABLE_NAME_FULL := :V_SCHEMA_NAME ||''.''||:V_RAW_TABLE;
    V_HIST_TABLE := :V_BASE_NAME || :V_HIST_SUFFIX;
    V_HIST_TABLE_NAME_FULL := :V_SCHEMA_NAME ||''.''||:V_HIST_TABLE;

    SELECT DYNAMIC_RAW_IND INTO V_DYNAMIC_RAW
    FROM CONFIG_ALL.STAGE_FILE_LOAD_TYPE
    WHERE BASE_NAME = :V_BASE_NAME;

    --IF DYNAMIC RAW = FALSE (CREATE FROM HIST) VALIDATE HIST ACTUALLY EXISTS, IF IT DOESNT THEN SET DYNAMIC RAW TO TRUE
    IF ((NOT V_DYNAMIC_RAW) AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :V_SCHEMA_NAME AND TABLE_NAME = :V_HIST_TABLE ))
    THEN
        V_DYNAMIC_RAW := TRUE;
    END IF;
 

---------------------------------------
--START OF DYNAMIC INPUT 
IF (:V_DYNAMIC_RAW)
THEN 

    -- Check if file is valid for Infer schema
    V_SQL := ''
     SELECT CASE WHEN array_size(CN)  = 0 THEN FALSE ELSE TRUE END AS V_CHECK
        FROM (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID) AS CN 
                    FROM TABLE(
                        INFER_SCHEMA(
                    LOCATION => '''''' || V_STAGE_PATH_WITH_FILE || '''''',
                    FILE_FORMAT => ''''''|| V_FILE_FORMAT || '''''',
                    MAX_RECORDS_PER_FILE => 1
                )
            )
        )
    '';
    EXECUTE IMMEDIATE :V_SQL;

    SELECT V_CHECK INTO :V_INFER_SCHEMA_VALID_CHECK
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    ---------------------------------------
    IF (:V_INFER_SCHEMA_VALID_CHECK)
    THEN 

    --CREATE AND LOAD TABLE--
    
        -- Create table from INFER_SCHEMA
        V_SQL := ''
            CREATE OR REPLACE TABLE '' || V_SCHEMA_NAME || ''.'' || V_RAW_TABLE || ''
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID)
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '''''' || V_STAGE_PATH_WITH_FILE || '''''',
                        FILE_FORMAT => ''''''|| V_FILE_FORMAT || ''''''
                    )
                )
            )
        '';
        EXECUTE IMMEDIATE :V_SQL;
        
    
        -- APPLY DATA TYPE OVERRIDE(S) IF ANY EXIST FOR GIVEN TABLE START
        IF (EXISTS (
            SELECT 1 FROM CONFIG_ALL.SCHEMA_DATA_TYPE_OVERRIDE_CONFIG
            WHERE SCHEMA_NAME = :V_SCHEMA_NAME
            AND TABLE_NAME = :V_BASE_NAME
            AND CURRENT_DATE() >= START_DT
            AND CURRENT_DATE() <= END_DT
        )) THEN
    
            WITH ORIG_COLUMNS AS (
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE || 
                    CASE 
                        WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN ''('' || CHARACTER_MAXIMUM_LENGTH || '')''
                        WHEN NUMERIC_PRECISION IS NOT NULL THEN ''('' || NUMERIC_PRECISION || '','' || NUMERIC_SCALE || '')''
                        ELSE ''''
                    END AS FULL_TYPE,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :V_SCHEMA_NAME
                AND TABLE_NAME = :V_BASE_NAME
            ),
            FINAL_COLUMNS AS (
                SELECT 
                    o.COLUMN_NAME,
                    COALESCE(c.DATA_TYPE, o.FULL_TYPE) AS DATA_TYPE,
                    ORDINAL_POSITION
                FROM ORIG_COLUMNS o
                LEFT JOIN CONFIG_ALL.SCHEMA_DATA_TYPE_OVERRIDE_CONFIG c
                ON c.TABLE_NAME = :V_BASE_NAME 
                AND c.COLUMN_NAME = o.COLUMN_NAME
            )
            SELECT 
                ''CREATE OR REPLACE TABLE '' || :V_SCHEMA_NAME || ''.'' || :V_RAW_TABLE || '' ('' ||
                LISTAGG(COLUMN_NAME || '' '' || DATA_TYPE, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION) ||
                '');''
                INTO :V_SQL
            FROM FINAL_COLUMNS;
            
            EXECUTE IMMEDIATE V_SQL;
            
        END IF;
        -- APPLY DATA TYPE OVERRIDE(S) IF ANY EXIST FOR GIVEN TABLE END
    
        -- Load data into the table
        V_SQL := ''
            COPY INTO '' || V_SCHEMA_NAME || ''.'' || V_RAW_TABLE || ''
            FROM '' || V_STAGE_PATH_WITH_FILE || ''
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            FILE_FORMAT = (FORMAT_NAME = ''''''|| V_FILE_FORMAT || '''''')
        '';
        EXECUTE IMMEDIATE :V_SQL;

    END IF; -- END OF VALID SCHEMA BEHAVIOR
    ---------------------------------------
 ELSE

        CREATE OR REPLACE TABLE IDENTIFIER(:V_RAW_TABLE_NAME_FULL) LIKE IDENTIFIER(:V_HIST_TABLE_NAME_FULL) ENABLE_SCHEMA_EVOLUTION=TRUE;
        ALTER TABLE IDENTIFIER(:V_RAW_TABLE_NAME_FULL) DROP COLUMN IF EXISTS CALC_DT;

        V_SQL := ''
        COPY INTO '' || V_SCHEMA_NAME || ''.'' || V_RAW_TABLE || ''
        FROM '' || V_STAGE_PATH_WITH_FILE || ''
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        FILE_FORMAT = (FORMAT_NAME = ''''''|| V_FILE_FORMAT || '''''')
        '';
        
        EXECUTE IMMEDIATE :V_SQL;
        
END IF; --END OF DYNAMIC INPUT
---------------------------------------
 
    EXECUTE IMMEDIATE ''LIST '' || V_STAGE_PATH_WITH_FILE;
    FILE_NAMES := (SELECT LISTAGG(SPLIT_PART("name",''/'',-1), '', '') FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

    -- Merge into metadata mapping table
        MERGE INTO CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW tgt
        USING (SELECT :V_STAGE_PATH  AS STAGE_PATH,
                      :FILE_NAMES AS FILE_NAMES,
                      :V_BASE_NAME  AS BASE_NAME,
                      :V_RAW_TABLE AS RAW_TABLE,
                      :V_HIST_TABLE AS HIST_TABLE,
                      :V_CURRENT_TS AS LOAD_DTTM,
                      :V_CALC_DT AS CALC_DT,
                      FALSE AS HIST_LOADED,
                      TRUE AS RAW_LOADED) src
        ON tgt.STAGE_PATH = src.STAGE_PATH
           AND tgt.BASE_NAME = src.BASE_NAME
           AND tgt.CALC_DT = src.CALC_DT
        WHEN MATCHED THEN
            UPDATE SET
                LAST_LOAD_DTTM = src.LOAD_DTTM,
                LAST_UPDATE_DTTM = src.LOAD_DTTM,
                BASE_NAME = src.BASE_NAME
        WHEN NOT MATCHED THEN
            INSERT (STAGE_PATH, FILE_NAMES, BASE_NAME, RAW_TABLE, HIST_TABLE, ENABLE_IND, LAST_LOAD_DTTM, LAST_UPDATE_DTTM, CALC_DT, HIST_LOADED, RAW_LOADED)
            VALUES (src.STAGE_PATH, src.FILE_NAMES, src.BASE_NAME, src.RAW_TABLE, src.HIST_TABLE, ''Y'', src.LOAD_DTTM, :V_CURRENT_TS, src.CALC_DT, src.HIST_LOADED, src.RAW_LOADED);

    --UPDATE MAPPING TABLE TO N IF SCHEMA IS NOT VALID--
    IF (NOT :V_INFER_SCHEMA_VALID_CHECK)
    THEN 
        UPDATE CONFIG_ALL.STAGE_FILE_TABLE_MAPPING_PW
            SET ENABLE_IND=''N'',RAW_LOADED=FALSE
        WHERE BASE_NAME = :V_BASE_NAME
        AND LAST_LOAD_DTTM = :V_CURRENT_TS
        AND CALC_DT = :V_CALC_DT;
    END IF;
    
    RETURN (:V_RAW_TABLE || '' created and loaded successfully.'');
END;
';