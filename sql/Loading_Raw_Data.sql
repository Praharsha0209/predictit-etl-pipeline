-- =====================================================
-- TWO-STEP APPROACH: Load Raw, Then Parse
-- =====================================================

USE DATABASE ETL_DB;
USE SCHEMA RAW_DATA;

-- Step 1: Create temp table for raw JSON
CREATE OR REPLACE TEMPORARY TABLE RAW_JSON_TEMP (
    RAW_DATA VARIANT,
    FILE_NAME VARCHAR,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Step 2: Load entire JSON files as VARIANT
COPY INTO RAW_JSON_TEMP (RAW_DATA, FILE_NAME)
FROM (
    SELECT 
        $1,
        METADATA$FILENAME
    FROM @ETL_DB.RAW_DATA.PREDICTIT_S3_STAGE
)
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*\.json'
FORCE = TRUE;

-- Step 3: Check what we loaded
SELECT COUNT(*) as files_loaded FROM RAW_JSON_TEMP;

-- Step 4: See the structure
SELECT RAW_DATA FROM RAW_JSON_TEMP LIMIT 1;

-- Step 5: Now parse and insert into PREDICTIT_RAW
TRUNCATE TABLE ETL_DB.RAW_DATA.PREDICTIT_RAW;

INSERT INTO ETL_DB.RAW_DATA.PREDICTIT_RAW (
    MARKET_ID,
    MARKET_NAME,
    SHORT_NAME,
    MARKET_URL,
    MARKET_STATUS,
    CONTRACT_DATA,
    EXTRACTION_TIMESTAMP
)
SELECT
    market.value:id::INTEGER,
    market.value:name::VARCHAR,
    market.value:shortName::VARCHAR,
    market.value:url::VARCHAR,
    COALESCE(market.value:status::VARCHAR, 'Unknown'),
    market.value:contracts::VARIANT,
    r.RAW_DATA:extracted_at::TIMESTAMP_NTZ
FROM RAW_JSON_TEMP r,
LATERAL FLATTEN(input => r.RAW_DATA:data:markets) market;

-- Step 6: Verify it worked!
SELECT COUNT(*) as loaded_rows 
FROM ETL_DB.RAW_DATA.PREDICTIT_RAW;

-- Step 7: See the actual data
SELECT 
    MARKET_ID,
    MARKET_NAME,
    SHORT_NAME,
    MARKET_STATUS,
    CASE 
        WHEN CONTRACT_DATA IS NOT NULL THEN ARRAY_SIZE(CONTRACT_DATA)
        ELSE 0
    END as num_contracts,
    EXTRACTION_TIMESTAMP
FROM ETL_DB.RAW_DATA.PREDICTIT_RAW
LIMIT 10;

-- Step 8: Detailed check
SELECT * 
FROM ETL_DB.RAW_DATA.PREDICTIT_RAW 
LIMIT 3;

-- Step 9: Verify we have real data (not NULLs)
SELECT 
    'Data Check' as test,
    COUNT(*) as total_rows,
    SUM(CASE WHEN MARKET_ID IS NOT NULL THEN 1 ELSE 0 END) as rows_with_market_id,
    SUM(CASE WHEN MARKET_NAME IS NOT NULL THEN 1 ELSE 0 END) as rows_with_market_name,
    SUM(CASE WHEN CONTRACT_DATA IS NOT NULL THEN 1 ELSE 0 END) as rows_with_contracts,
    MIN(MARKET_ID) as min_market_id,
    MAX(MARKET_ID) as max_market_id
FROM ETL_DB.RAW_DATA.PREDICTIT_RAW;