-- =====================================================
-- POPULATE ALL ANALYTICS TABLES WITH REAL DATA
-- =====================================================

USE DATABASE ETL_DB;
USE SCHEMA ANALYTICS;

-- =====================================================
-- STEP 1: POPULATE MARKET_SUMMARY
-- =====================================================

TRUNCATE TABLE ETL_DB.ANALYTICS.MARKET_SUMMARY;

INSERT INTO ETL_DB.ANALYTICS.MARKET_SUMMARY (
    MARKET_ID,
    MARKET_NAME,
    SHORT_NAME,
    MARKET_URL,
    MARKET_STATUS,
    TOTAL_CONTRACTS,
    TOTAL_VOLUME,
    LAST_UPDATED
)
SELECT DISTINCT
    market_id,
    market_name,
    short_name,
    market_url,
    market_status,
    CASE 
        WHEN contract_data IS NOT NULL 
        THEN ARRAY_SIZE(contract_data) 
        ELSE 0 
    END AS total_contracts,
    0.00 AS total_volume,
    extraction_timestamp AS last_updated
FROM ETL_DB.RAW_DATA.PREDICTIT_RAW
WHERE market_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY extraction_timestamp DESC) = 1;

SELECT 'MARKET_SUMMARY populated' as status, COUNT(*) as row_count 
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY;

-- =====================================================
-- STEP 2: POPULATE CONTRACT_DETAILS
-- =====================================================

TRUNCATE TABLE ETL_DB.ANALYTICS.CONTRACT_DETAILS;

INSERT INTO ETL_DB.ANALYTICS.CONTRACT_DETAILS (
    CONTRACT_ID,
    MARKET_ID,
    CONTRACT_NAME,
    CONTRACT_STATUS,
    LAST_TRADE_PRICE,
    BEST_BUY_YES_COST,
    BEST_SELL_YES_COST,
    BEST_BUY_NO_COST,
    BEST_SELL_NO_COST,
    LAST_CLOSE_PRICE,
    DISPLAY_ORDER,
    LAST_UPDATED
)
SELECT
    contract.value:id::INTEGER AS contract_id,
    raw.market_id,
    contract.value:name::VARCHAR AS contract_name,
    contract.value:status::VARCHAR AS contract_status,
    contract.value:lastTradePrice::DECIMAL(10,4) AS last_trade_price,
    contract.value:bestBuyYesCost::DECIMAL(10,4) AS best_buy_yes_cost,
    contract.value:bestSellYesCost::DECIMAL(10,4) AS best_sell_yes_cost,
    contract.value:bestBuyNoCost::DECIMAL(10,4) AS best_buy_no_cost,
    contract.value:bestSellNoCost::DECIMAL(10,4) AS best_sell_no_cost,
    contract.value:lastClosePrice::DECIMAL(10,4) AS last_close_price,
    contract.value:displayOrder::INTEGER AS display_order,
    raw.extraction_timestamp AS last_updated
FROM ETL_DB.RAW_DATA.PREDICTIT_RAW raw,
LATERAL FLATTEN(input => raw.contract_data) contract
WHERE raw.market_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY contract.value:id 
    ORDER BY raw.extraction_timestamp DESC
) = 1;

SELECT 'CONTRACT_DETAILS populated' as status, COUNT(*) as row_count 
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS;

-- =====================================================
-- STEP 3: POPULATE DAILY_MARKET_METRICS
-- =====================================================

TRUNCATE TABLE ETL_DB.ANALYTICS.DAILY_MARKET_METRICS;

INSERT INTO ETL_DB.ANALYTICS.DAILY_MARKET_METRICS (
    MARKET_ID,
    METRIC_DATE,
    TOTAL_CONTRACTS,
    TOTAL_VOLUME,
    AVG_TRADE_PRICE,
    PRICE_VOLATILITY
)
SELECT
    m.MARKET_ID,
    CURRENT_DATE() AS METRIC_DATE,
    COUNT(DISTINCT c.CONTRACT_ID) AS TOTAL_CONTRACTS,
    COALESCE(SUM(c.LAST_TRADE_PRICE), 0) AS TOTAL_VOLUME,
    COALESCE(AVG(c.LAST_TRADE_PRICE), 0) AS AVG_TRADE_PRICE,
    COALESCE(STDDEV(c.LAST_TRADE_PRICE), 0) AS PRICE_VOLATILITY
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY m
LEFT JOIN ETL_DB.ANALYTICS.CONTRACT_DETAILS c ON m.MARKET_ID = c.MARKET_ID
WHERE m.MARKET_STATUS = 'Unknown'  -- Adjust if you find the actual status
GROUP BY m.MARKET_ID;

SELECT 'DAILY_MARKET_METRICS populated' as status, COUNT(*) as row_count 
FROM ETL_DB.ANALYTICS.DAILY_MARKET_METRICS;

-- =====================================================
-- VERIFICATION: Check all tables
-- =====================================================

SELECT 'FINAL VERIFICATION' as step;

SELECT 
    'MARKET_SUMMARY' as table_name, 
    COUNT(*) as row_count 
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY
UNION ALL
SELECT 
    'CONTRACT_DETAILS', 
    COUNT(*) 
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS
UNION ALL
SELECT 
    'DAILY_MARKET_METRICS', 
    COUNT(*) 
FROM ETL_DB.ANALYTICS.DAILY_MARKET_METRICS;

-- =====================================================
-- SAMPLE DATA FROM EACH TABLE
-- =====================================================

SELECT '=== MARKET_SUMMARY Sample ===' as info;
SELECT * FROM ETL_DB.ANALYTICS.MARKET_SUMMARY LIMIT 5;

SELECT '=== CONTRACT_DETAILS Sample ===' as info;
SELECT * FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS LIMIT 5;

SELECT '=== DAILY_MARKET_METRICS Sample ===' as info;
SELECT * FROM ETL_DB.ANALYTICS.DAILY_MARKET_METRICS LIMIT 5;

-- =====================================================
-- READY FOR ANALYSIS!
-- =====================================================

SELECT '🎉 ALL ANALYTICS TABLES POPULATED!' as final_message;
