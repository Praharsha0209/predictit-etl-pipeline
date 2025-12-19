-- =====================================================
-- PREDICTIT DATA ANALYSIS QUERIES
-- =====================================================

USE DATABASE ETL_DB;
USE SCHEMA ANALYTICS;

-- =====================================================
-- 1. Markets with Most Contracts
-- =====================================================
SELECT '=== TOP 10 MARKETS BY CONTRACT COUNT ===' as section;

SELECT 
    MARKET_NAME,
    TOTAL_CONTRACTS,
    MARKET_STATUS
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY
ORDER BY TOTAL_CONTRACTS DESC
LIMIT 10;

-- =====================================================
-- 2. Most Expensive vs Cheapest Contracts
-- =====================================================
SELECT '=== TOP 10 MOST EXPENSIVE CONTRACTS ===' as section;

SELECT 
    CONTRACT_NAME,
    LAST_TRADE_PRICE,
    CONTRACT_STATUS
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS
WHERE LAST_TRADE_PRICE IS NOT NULL
ORDER BY LAST_TRADE_PRICE DESC
LIMIT 10;

SELECT '=== TOP 10 CHEAPEST CONTRACTS ===' as section;

SELECT 
    CONTRACT_NAME,
    LAST_TRADE_PRICE,
    CONTRACT_STATUS
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS
WHERE LAST_TRADE_PRICE IS NOT NULL
  AND LAST_TRADE_PRICE > 0
ORDER BY LAST_TRADE_PRICE ASC
LIMIT 10;

-- =====================================================
-- 3. Best Trading Opportunities (Largest Spreads)
-- =====================================================
SELECT '=== TOP 10 BEST TRADING OPPORTUNITIES ===' as section;

SELECT 
    c.CONTRACT_NAME,
    m.MARKET_NAME,
    c.LAST_TRADE_PRICE,
    c.BEST_BUY_YES_COST,
    c.BEST_SELL_YES_COST,
    ROUND(c.BEST_SELL_YES_COST - c.BEST_BUY_YES_COST, 4) as spread,
    ROUND((c.BEST_SELL_YES_COST - c.BEST_BUY_YES_COST) / c.BEST_BUY_YES_COST * 100, 2) as spread_pct
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS c
JOIN ETL_DB.ANALYTICS.MARKET_SUMMARY m ON c.MARKET_ID = m.MARKET_ID
WHERE c.CONTRACT_STATUS = 'Open'
  AND c.BEST_BUY_YES_COST IS NOT NULL
  AND c.BEST_SELL_YES_COST IS NOT NULL
  AND c.BEST_BUY_YES_COST > 0
ORDER BY spread DESC
LIMIT 10;

-- =====================================================
-- 4. Market Volatility Rankings
-- =====================================================
SELECT '=== TOP 10 MOST VOLATILE MARKETS ===' as section;

SELECT 
    m.MARKET_NAME,
    mm.PRICE_VOLATILITY,
    mm.AVG_TRADE_PRICE,
    mm.TOTAL_CONTRACTS
FROM ETL_DB.ANALYTICS.DAILY_MARKET_METRICS mm
JOIN ETL_DB.ANALYTICS.MARKET_SUMMARY m ON mm.MARKET_ID = m.MARKET_ID
WHERE mm.PRICE_VOLATILITY IS NOT NULL
ORDER BY mm.PRICE_VOLATILITY DESC
LIMIT 10;

-- =====================================================
-- 5. Contract Status Distribution
-- =====================================================
SELECT '=== CONTRACT STATUS BREAKDOWN ===' as section;

SELECT 
    CONTRACT_STATUS,
    COUNT(*) as num_contracts,
    ROUND(AVG(LAST_TRADE_PRICE), 4) as avg_price,
    ROUND(MIN(LAST_TRADE_PRICE), 4) as min_price,
    ROUND(MAX(LAST_TRADE_PRICE), 4) as max_price
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS
GROUP BY CONTRACT_STATUS
ORDER BY num_contracts DESC;

-- =====================================================
-- 6. Price Distribution Analysis
-- =====================================================
SELECT '=== PRICE RANGE DISTRIBUTION ===' as section;

SELECT 
    CASE 
        WHEN LAST_TRADE_PRICE < 0.10 THEN '$0.00 - $0.10'
        WHEN LAST_TRADE_PRICE < 0.20 THEN '$0.10 - $0.20'
        WHEN LAST_TRADE_PRICE < 0.30 THEN '$0.20 - $0.30'
        WHEN LAST_TRADE_PRICE < 0.40 THEN '$0.30 - $0.40'
        WHEN LAST_TRADE_PRICE < 0.50 THEN '$0.40 - $0.50'
        WHEN LAST_TRADE_PRICE < 0.60 THEN '$0.50 - $0.60'
        WHEN LAST_TRADE_PRICE < 0.70 THEN '$0.60 - $0.70'
        WHEN LAST_TRADE_PRICE < 0.80 THEN '$0.70 - $0.80'
        WHEN LAST_TRADE_PRICE < 0.90 THEN '$0.80 - $0.90'
        ELSE '$0.90 - $1.00'
    END as price_range,
    COUNT(*) as num_contracts,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS
WHERE LAST_TRADE_PRICE IS NOT NULL
GROUP BY 
    CASE 
        WHEN LAST_TRADE_PRICE < 0.10 THEN '$0.00 - $0.10'
        WHEN LAST_TRADE_PRICE < 0.20 THEN '$0.10 - $0.20'
        WHEN LAST_TRADE_PRICE < 0.30 THEN '$0.20 - $0.30'
        WHEN LAST_TRADE_PRICE < 0.40 THEN '$0.30 - $0.40'
        WHEN LAST_TRADE_PRICE < 0.50 THEN '$0.40 - $0.50'
        WHEN LAST_TRADE_PRICE < 0.60 THEN '$0.50 - $0.60'
        WHEN LAST_TRADE_PRICE < 0.70 THEN '$0.60 - $0.70'
        WHEN LAST_TRADE_PRICE < 0.80 THEN '$0.70 - $0.80'
        WHEN LAST_TRADE_PRICE < 0.90 THEN '$0.80 - $0.90'
        ELSE '$0.90 - $1.00'
    END
ORDER BY price_range;

-- =====================================================
-- 7. Markets with Highest Average Contract Price
-- =====================================================
SELECT '=== MARKETS WITH HIGHEST AVG CONTRACT PRICE ===' as section;

SELECT 
    m.MARKET_NAME,
    m.TOTAL_CONTRACTS,
    ROUND(AVG(c.LAST_TRADE_PRICE), 4) as avg_contract_price,
    ROUND(MIN(c.LAST_TRADE_PRICE), 4) as min_price,
    ROUND(MAX(c.LAST_TRADE_PRICE), 4) as max_price
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY m
JOIN ETL_DB.ANALYTICS.CONTRACT_DETAILS c ON m.MARKET_ID = c.MARKET_ID
WHERE c.LAST_TRADE_PRICE IS NOT NULL
GROUP BY m.MARKET_NAME, m.TOTAL_CONTRACTS
HAVING COUNT(c.CONTRACT_ID) >= 3  -- At least 3 contracts
ORDER BY avg_contract_price DESC
LIMIT 10;

-- =====================================================
-- 8. Overall Market Statistics
-- =====================================================
SELECT '=== OVERALL MARKET STATISTICS ===' as section;

SELECT 
    COUNT(DISTINCT m.MARKET_ID) as total_markets,
    SUM(m.TOTAL_CONTRACTS) as total_contract_slots,
    COUNT(c.CONTRACT_ID) as actual_contracts,
    ROUND(AVG(c.LAST_TRADE_PRICE), 4) as overall_avg_price,
    ROUND(MIN(c.LAST_TRADE_PRICE), 4) as lowest_price,
    ROUND(MAX(c.LAST_TRADE_PRICE), 4) as highest_price,
    ROUND(STDDEV(c.LAST_TRADE_PRICE), 4) as price_stddev
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY m
LEFT JOIN ETL_DB.ANALYTICS.CONTRACT_DETAILS c ON m.MARKET_ID = c.MARKET_ID;