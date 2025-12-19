-- =====================================================
-- Snowflake Data Transformation Script
-- Transforms raw data into analytics-ready tables
-- =====================================================

USE DATABASE ETL_DB;

-- =====================================================
-- 1. Transform Raw Data to Market Summary
-- =====================================================

USE SCHEMA ANALYTICS;

-- Merge into Market Summary
MERGE INTO ETL_DB.ANALYTICS.MARKET_SUMMARY AS target
USING (
    SELECT DISTINCT
        market_id,
        market_name,
        short_name,
        market_url,
        market_status,
        ARRAY_SIZE(contract_data) AS total_contracts,
        0.00 AS total_volume,  -- Calculate if volume data available
        extraction_timestamp AS last_updated
    FROM ETL_DB.RAW_DATA.PREDICTIT_RAW
    WHERE market_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY extraction_timestamp DESC) = 1
) AS source
ON target.market_id = source.market_id
WHEN MATCHED THEN
    UPDATE SET
        target.market_name = source.market_name,
        target.short_name = source.short_name,
        target.market_url = source.market_url,
        target.market_status = source.market_status,
        target.total_contracts = source.total_contracts,
        target.last_updated = source.last_updated
WHEN NOT MATCHED THEN
    INSERT (
        market_id,
        market_name,
        short_name,
        market_url,
        market_status,
        total_contracts,
        total_volume,
        last_updated
    )
    VALUES (
        source.market_id,
        source.market_name,
        source.short_name,
        source.market_url,
        source.market_status,
        source.total_contracts,
        source.total_volume,
        source.last_updated
    );

-- =====================================================
-- 2. Transform Raw Data to Contract Details
-- =====================================================

-- Flatten contract data from VARIANT column
MERGE INTO ETL_DB.ANALYTICS.CONTRACT_DETAILS AS target 
USING (
    SELECT
        contract.value:contractId::INTEGER AS contract_id,
        raw.market_id,
        contract.value:contractName::VARCHAR AS contract_name,
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
        PARTITION BY contract.value:contractId 
        ORDER BY raw.extraction_timestamp DESC
    ) = 1
) AS source
ON target.contract_id = source.contract_id
WHEN MATCHED THEN
    UPDATE SET
        target.contract_name = source.contract_name,
        target.contract_status = source.contract_status,
        target.last_trade_price = source.last_trade_price,
        target.best_buy_yes_cost = source.best_buy_yes_cost,
        target.best_sell_yes_cost = source.best_sell_yes_cost,
        target.best_buy_no_cost = source.best_buy_no_cost,
        target.best_sell_no_cost = source.best_sell_no_cost,
        target.last_close_price = source.last_close_price,
        target.display_order = source.display_order,
        target.last_updated = source.last_updated
WHEN NOT MATCHED THEN
    INSERT (
        contract_id,
        market_id,
        contract_name,
        contract_status,
        last_trade_price,
        best_buy_yes_cost,
        best_sell_yes_cost,
        best_buy_no_cost,
        best_sell_no_cost,
        last_close_price,
        display_order,
        last_updated
    )
    VALUES (
        source.contract_id,
        source.market_id,
        source.contract_name,
        source.contract_status,
        source.last_trade_price,
        source.best_buy_yes_cost,
        source.best_sell_yes_cost,
        source.best_buy_no_cost,
        source.best_sell_no_cost,
        source.last_close_price,
        source.display_order,
        source.last_updated
    );

-- =====================================================
-- 3. Create Daily Metrics
-- =====================================================

-- Insert daily metrics (aggregated by date)
INSERT INTO DAILY_MARKET_METRICS (
    metric_date,
    market_id,
    total_volume,
    avg_last_trade_price,
    num_active_contracts
)
SELECT
    CURRENT_DATE() AS metric_date,
    market_id,
    SUM(total_volume) AS total_volume,
    AVG(last_trade_price) AS avg_last_trade_price,
    COUNT(DISTINCT contract_id) AS num_active_contracts
FROM (
    SELECT
        m.market_id,
        m.total_volume,
        c.contract_id,
        c.last_trade_price
    FROM MARKET_SUMMARY m
    LEFT JOIN CONTRACT_DETAILS c ON m.market_id = c.market_id
    WHERE m.market_status = 'Open'
)
GROUP BY market_id
ON CONFLICT (metric_date, market_id) DO NOTHING;

-- =====================================================
-- 4. Data Quality and Cleanup
-- =====================================================

-- Remove duplicate records from raw table (keep most recent)
DELETE FROM ETL_DB.RAW_DATA.PREDICTIT_RAW
WHERE (market_id, extraction_timestamp) NOT IN (
    SELECT market_id, MAX(extraction_timestamp)
    FROM ETL_DB.RAW_DATA.PREDICTIT_RAW
    GROUP BY market_id
);

-- Mark closed markets
UPDATE MARKET_SUMMARY
SET market_status = 'Closed'
WHERE market_id NOT IN (
    SELECT DISTINCT market_id
    FROM ETL_DB.RAW_DATA.PREDICTIT_RAW
    WHERE extraction_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
);

-- =====================================================
-- 5. Create/Refresh Materialized Views (if supported)
-- =====================================================

-- Create a table that acts as a materialized view for market trends
CREATE OR REPLACE TABLE MARKET_TRENDS AS
SELECT
    m.market_id,
    m.market_name,
    m.market_status,
    COUNT(DISTINCT c.contract_id) AS total_contracts,
    AVG(c.last_trade_price) AS avg_price,
    MIN(c.last_trade_price) AS min_price,
    MAX(c.last_trade_price) AS max_price,
    STDDEV(c.last_trade_price) AS price_volatility,
    m.last_updated
FROM MARKET_SUMMARY m
LEFT JOIN CONTRACT_DETAILS c ON m.market_id = c.market_id
GROUP BY m.market_id, m.market_name, m.market_status, m.last_updated;

-- =====================================================
-- 6. Update Statistics (optional, for query optimization)
-- =====================================================

-- Analyze tables for query optimization
-- Note: Snowflake handles statistics automatically, but these are available if needed

-- ANALYZE TABLE MARKET_SUMMARY COMPUTE STATISTICS;
-- ANALYZE TABLE CONTRACT_DETAILS COMPUTE STATISTICS;
-- ANALYZE TABLE DAILY_MARKET_METRICS COMPUTE STATISTICS;

-- =====================================================
-- Verification Queries
-- =====================================================

-- Check record counts
SELECT 'MARKET_SUMMARY' AS table_name, COUNT(*) AS row_count FROM MARKET_SUMMARY
UNION ALL
SELECT 'CONTRACT_DETAILS', COUNT(*) FROM CONTRACT_DETAILS
UNION ALL
SELECT 'DAILY_MARKET_METRICS', COUNT(*) FROM DAILY_MARKET_METRICS;

-- Check for data quality issues
SELECT 
    'Null market_id in MARKET_SUMMARY' AS check_name,
    COUNT(*) AS issue_count
FROM MARKET_SUMMARY
WHERE market_id IS NULL
UNION ALL
SELECT 
    'Null contract_id in CONTRACT_DETAILS',
    COUNT(*)
FROM CONTRACT_DETAILS
WHERE contract_id IS NULL;

-- Show sample of transformed data
SELECT * FROM MARKET_SUMMARY LIMIT 5;
SELECT * FROM CONTRACT_DETAILS LIMIT 5;

COMMIT;
```

---

## ✅ Summary - Both SQL Files:

You now have both SQL scripts:

1. ✅ **`create_tables.sql`** (174 lines)
   - Creates database schemas (RAW_DATA, ANALYTICS)
   - Creates 4 tables (PREDICTIT_RAW, MARKET_SUMMARY, CONTRACT_DETAILS, DAILY_MARKET_METRICS)
   - Creates 3 views (ACTIVE_MARKETS, TOP_VOLUME_MARKETS, CONTRACT_PRICE_MOVEMENTS)
   - Sets up file formats and permissions

2. ✅ **`transformations.sql`** (244 lines)
   - Transforms raw JSON data into structured tables
   - Uses MERGE statements for upserts
   - Flattens nested JSON (VARIANT column)
   - Creates daily aggregations
   - Includes data quality checks and cleanup

---

## 📁 Where to Put Them:
```
airflow-etl/
  └── sql/
      ├── create_tables.sql       ← File 1
      └── transformations.sql     ← File 2