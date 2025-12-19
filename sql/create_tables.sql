-- =====================================================
-- Snowflake Table Creation Script
-- =====================================================

-- Use the appropriate database and schema
USE DATABASE ETL_DB;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. Create Raw Data Table for PredictIt Markets
-- =====================================================

CREATE TABLE IF NOT EXISTS PREDICTIT_RAW (
    market_id INTEGER,
    market_name VARCHAR(500),
    short_name VARCHAR(200),
    market_url VARCHAR(500),
    market_status VARCHAR(50),
    contract_data VARIANT,
    extraction_timestamp TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500)
);

-- =====================================================
-- 2. Create Analytics Schema and Tables
-- =====================================================

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- Market Summary Table
CREATE TABLE IF NOT EXISTS MARKET_SUMMARY (
    market_id INTEGER PRIMARY KEY,
    market_name VARCHAR(500),
    short_name VARCHAR(200),
    market_url VARCHAR(500),
    market_status VARCHAR(50),
    total_contracts INTEGER,
    total_volume DECIMAL(18,2),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Contract Details Table
CREATE TABLE IF NOT EXISTS CONTRACT_DETAILS (
    contract_id INTEGER PRIMARY KEY,
    market_id INTEGER,
    contract_name VARCHAR(500),
    contract_status VARCHAR(50),
    last_trade_price DECIMAL(10,4),
    best_buy_yes_cost DECIMAL(10,4),
    best_sell_yes_cost DECIMAL(10,4),
    best_buy_no_cost DECIMAL(10,4),
    best_sell_no_cost DECIMAL(10,4),
    last_close_price DECIMAL(10,4),
    display_order INTEGER,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (market_id) REFERENCES MARKET_SUMMARY(market_id)
);

-- Daily Market Metrics Table (for time series analysis)
CREATE TABLE IF NOT EXISTS DAILY_MARKET_METRICS (
    metric_date DATE,
    market_id INTEGER,
    total_volume DECIMAL(18,2),
    avg_last_trade_price DECIMAL(10,4),
    num_active_contracts INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (metric_date, market_id)
);

-- =====================================================
-- 3. Create File Format for JSON data
-- =====================================================

USE SCHEMA RAW_DATA;

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = 'AUTO';

-- =====================================================
-- 4. Create Views for Easy Access
-- =====================================================

USE SCHEMA ANALYTICS;

-- View: Active Markets
CREATE OR REPLACE VIEW ACTIVE_MARKETS AS
SELECT 
    market_id,
    market_name,
    short_name,
    market_status,
    total_contracts,
    total_volume,
    last_updated
FROM MARKET_SUMMARY
WHERE market_status = 'Open';

-- View: Top Volume Markets
CREATE OR REPLACE VIEW TOP_VOLUME_MARKETS AS
SELECT 
    market_id,
    market_name,
    total_volume,
    total_contracts,
    last_updated
FROM MARKET_SUMMARY
ORDER BY total_volume DESC
LIMIT 20;

-- View: Contract Price Movements
CREATE OR REPLACE VIEW CONTRACT_PRICE_MOVEMENTS AS
SELECT 
    c.contract_id,
    c.contract_name,
    m.market_name,
    c.last_trade_price,
    c.last_close_price,
    (c.last_trade_price - c.last_close_price) AS price_change,
    CASE 
        WHEN c.last_close_price > 0 
        THEN ((c.last_trade_price - c.last_close_price) / c.last_close_price) * 100
        ELSE 0
    END AS price_change_pct,
    c.last_updated
FROM CONTRACT_DETAILS c
JOIN MARKET_SUMMARY m ON c.market_id = m.market_id
WHERE c.contract_status = 'Open';

-- =====================================================
-- 5. Grant Permissions (adjust as needed)
-- =====================================================

-- Grant usage on database
GRANT USAGE ON DATABASE ETL_DB TO ROLE PUBLIC;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA RAW_DATA TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PUBLIC;

-- Grant select on tables
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE PUBLIC;

-- Grant select on views
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE PUBLIC;

-- =====================================================
-- 6. Create Sequences (if needed for surrogate keys)
-- =====================================================

USE SCHEMA ANALYTICS;

CREATE SEQUENCE IF NOT EXISTS market_seq START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS contract_seq START = 1 INCREMENT = 1;

-- =====================================================
-- Verification Queries
-- =====================================================

-- Check if tables were created successfully
SHOW TABLES IN SCHEMA RAW_DATA;
SHOW TABLES IN SCHEMA ANALYTICS;

-- Check table structures
DESCRIBE TABLE PREDICTIT_RAW;
DESCRIBE TABLE MARKET_SUMMARY;
DESCRIBE TABLE CONTRACT_DETAILS;

COMMIT;