-- =====================================================
-- PREDICTIT DATA ANALYSIS - COMPLETE ANALYSIS
-- Fresh data from automated pipeline
-- =====================================================

USE DATABASE ETL_DB;
USE SCHEMA ANALYTICS;

-- =====================================================
-- SECTION 1: DATA OVERVIEW
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 1: DATA OVERVIEW' as section;
SELECT '========================================' as section;

-- Overall Statistics
SELECT 
    'Overall Statistics' as metric_type,
    COUNT(DISTINCT m.MARKET_ID) as total_markets,
    SUM(m.TOTAL_CONTRACTS) as total_contract_slots,
    COUNT(c.CONTRACT_ID) as actual_contracts,
    ROUND(AVG(c.LAST_TRADE_PRICE), 4) as overall_avg_price,
    ROUND(MIN(c.LAST_TRADE_PRICE), 4) as lowest_price,
    ROUND(MAX(c.LAST_TRADE_PRICE), 4) as highest_price,
    ROUND(STDDEV(c.LAST_TRADE_PRICE), 4) as price_stddev
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY m
LEFT JOIN ETL_DB.ANALYTICS.CONTRACT_DETAILS c ON m.MARKET_ID = c.MARKET_ID;

-- Table Row Counts
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
FROM ETL_DB.ANALYTICS.DAILY_MARKET_METRICS
UNION ALL
SELECT 
    'RAW_DATA.PREDICTIT_RAW', 
    COUNT(*) 
FROM ETL_DB.RAW_DATA.PREDICTIT_RAW;

-- Data Freshness Check
SELECT 
    'Data Freshness' as check_type,
    MAX(LAST_UPDATED) as latest_data_timestamp,
    DATEDIFF('hour', MAX(LAST_UPDATED), CURRENT_TIMESTAMP()) as hours_since_update
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY;

-- =====================================================
-- SECTION 2: TOP MARKETS ANALYSIS
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 2: TOP MARKETS' as section;
SELECT '========================================' as section;

-- Top 15 Markets by Contract Count
SELECT 
    'Top 15 Markets by Contract Count' as analysis;
    
SELECT 
    MARKET_NAME,
    TOTAL_CONTRACTS,
    MARKET_STATUS,
    SHORT_NAME
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY
ORDER BY TOTAL_CONTRACTS DESC
LIMIT 15;

-- Markets with Most Active Trading (most contracts with prices)
SELECT 
    'Markets with Most Active Trading' as analysis;

SELECT 
    m.MARKET_NAME,
    m.TOTAL_CONTRACTS,
    COUNT(c.CONTRACT_ID) as active_contracts,
    ROUND(AVG(c.LAST_TRADE_PRICE), 4) as avg_price,
    ROUND(SUM(c.LAST_TRADE_PRICE), 4) as total_volume
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY m
JOIN ETL_DB.ANALYTICS.CONTRACT_DETAILS c ON m.MARKET_ID = c.MARKET_ID
WHERE c.LAST_TRADE_PRICE IS NOT NULL
GROUP BY m.MARKET_NAME, m.TOTAL_CONTRACTS
ORDER BY active_contracts DESC
LIMIT 15;

-- =====================================================
-- SECTION 3: CONTRACT PRICE ANALYSIS
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 3: PRICE ANALYSIS' as section;
SELECT '========================================' as section;

-- Top 10 Most Expensive Contracts
SELECT 
    'Top 10 Most Expensive Contracts' as analysis;

SELECT 
    c.CONTRACT_NAME,
    m.MARKET_NAME,
    c.LAST_TRADE_PRICE,
    c.CONTRACT_STATUS,
    c.BEST_BUY_YES_COST,
    c.BEST_SELL_YES_COST
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS c
JOIN ETL_DB.ANALYTICS.MARKET_SUMMARY m ON c.MARKET_ID = m.MARKET_ID
WHERE c.LAST_TRADE_PRICE IS NOT NULL
ORDER BY c.LAST_TRADE_PRICE DESC
LIMIT 10;

-- Top 10 Cheapest Contracts (excluding zeros)
SELECT 
    'Top 10 Cheapest Contracts (excluding $0)' as analysis;

SELECT 
    c.CONTRACT_NAME,
    m.MARKET_NAME,
    c.LAST_TRADE_PRICE,
    c.CONTRACT_STATUS
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS c
JOIN ETL_DB.ANALYTICS.MARKET_SUMMARY m ON c.MARKET_ID = m.MARKET_ID
WHERE c.LAST_TRADE_PRICE IS NOT NULL
  AND c.LAST_TRADE_PRICE > 0
ORDER BY c.LAST_TRADE_PRICE ASC
LIMIT 10;

-- Contract Status Distribution
SELECT 
    'Contract Status Distribution' as analysis;

SELECT 
    CONTRACT_STATUS,
    COUNT(*) as num_contracts,
    ROUND(AVG(LAST_TRADE_PRICE), 4) as avg_price,
    ROUND(MIN(LAST_TRADE_PRICE), 4) as min_price,
    ROUND(MAX(LAST_TRADE_PRICE), 4) as max_price,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS
GROUP BY CONTRACT_STATUS
ORDER BY num_contracts DESC;

-- =====================================================
-- SECTION 4: PRICE DISTRIBUTION
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 4: PRICE DISTRIBUTION' as section;
SELECT '========================================' as section;

SELECT 
    'Price Range Distribution' as analysis;

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
-- SECTION 5: TRADING OPPORTUNITIES
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 5: TRADING OPPORTUNITIES' as section;
SELECT '========================================' as section;

-- Best Trading Opportunities (Largest Spreads)
SELECT 
    'Top 15 Best Trading Opportunities (Largest Spreads)' as analysis;

SELECT 
    c.CONTRACT_NAME,
    m.MARKET_NAME,
    c.LAST_TRADE_PRICE,
    c.BEST_BUY_YES_COST,
    c.BEST_SELL_YES_COST,
    ROUND(c.BEST_SELL_YES_COST - c.BEST_BUY_YES_COST, 4) as spread,
    ROUND((c.BEST_SELL_YES_COST - c.BEST_BUY_YES_COST) / NULLIF(c.BEST_BUY_YES_COST, 0) * 100, 2) as spread_pct,
    c.CONTRACT_STATUS
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS c
JOIN ETL_DB.ANALYTICS.MARKET_SUMMARY m ON c.MARKET_ID = m.MARKET_ID
WHERE c.CONTRACT_STATUS = 'Open'
  AND c.BEST_BUY_YES_COST IS NOT NULL
  AND c.BEST_SELL_YES_COST IS NOT NULL
  AND c.BEST_BUY_YES_COST > 0
  AND c.BEST_SELL_YES_COST > c.BEST_BUY_YES_COST
ORDER BY spread DESC
LIMIT 15;

-- Contracts with Highest Buy/Sell Imbalance
SELECT 
    'Contracts with Highest Buy/Sell Imbalance' as analysis;

SELECT 
    c.CONTRACT_NAME,
    c.BEST_BUY_YES_COST as buy_yes,
    c.BEST_BUY_NO_COST as buy_no,
    c.BEST_SELL_YES_COST as sell_yes,
    c.BEST_SELL_NO_COST as sell_no,
    ROUND(c.BEST_BUY_YES_COST + c.BEST_BUY_NO_COST, 4) as total_buy,
    ROUND(c.BEST_SELL_YES_COST + c.BEST_SELL_NO_COST, 4) as total_sell
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS c
WHERE c.BEST_BUY_YES_COST IS NOT NULL
  AND c.BEST_BUY_NO_COST IS NOT NULL
  AND c.CONTRACT_STATUS = 'Open'
ORDER BY (c.BEST_BUY_YES_COST + c.BEST_BUY_NO_COST) DESC
LIMIT 10;

-- =====================================================
-- SECTION 6: MARKET VOLATILITY
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 6: VOLATILITY ANALYSIS' as section;
SELECT '========================================' as section;

-- Most Volatile Markets
SELECT 
    'Top 15 Most Volatile Markets' as analysis;

SELECT 
    m.MARKET_NAME,
    mm.PRICE_VOLATILITY,
    mm.AVG_TRADE_PRICE,
    mm.TOTAL_CONTRACTS,
    mm.METRIC_DATE
FROM ETL_DB.ANALYTICS.DAILY_MARKET_METRICS mm
JOIN ETL_DB.ANALYTICS.MARKET_SUMMARY m ON mm.MARKET_ID = m.MARKET_ID
WHERE mm.PRICE_VOLATILITY IS NOT NULL
  AND mm.PRICE_VOLATILITY > 0
ORDER BY mm.PRICE_VOLATILITY DESC
LIMIT 15;

-- Markets with Highest Average Prices
SELECT 
    'Markets with Highest Average Contract Prices' as analysis;

SELECT 
    m.MARKET_NAME,
    m.TOTAL_CONTRACTS,
    COUNT(c.CONTRACT_ID) as num_contracts,
    ROUND(AVG(c.LAST_TRADE_PRICE), 4) as avg_contract_price,
    ROUND(MIN(c.LAST_TRADE_PRICE), 4) as min_price,
    ROUND(MAX(c.LAST_TRADE_PRICE), 4) as max_price,
    ROUND(STDDEV(c.LAST_TRADE_PRICE), 4) as price_stddev
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY m
JOIN ETL_DB.ANALYTICS.CONTRACT_DETAILS c ON m.MARKET_ID = c.MARKET_ID
WHERE c.LAST_TRADE_PRICE IS NOT NULL
GROUP BY m.MARKET_NAME, m.TOTAL_CONTRACTS
HAVING COUNT(c.CONTRACT_ID) >= 3
ORDER BY avg_contract_price DESC
LIMIT 15;

-- =====================================================
-- SECTION 7: MARKET CONCENTRATION
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 7: MARKET CONCENTRATION' as section;
SELECT '========================================' as section;

-- Contracts per Market Distribution
SELECT 
    'Contracts per Market Distribution' as analysis;

SELECT 
    TOTAL_CONTRACTS as contracts_per_market,
    COUNT(*) as num_markets,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage_of_markets
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY
GROUP BY TOTAL_CONTRACTS
ORDER BY TOTAL_CONTRACTS DESC
LIMIT 20;

-- Market Status Distribution
SELECT 
    'Market Status Distribution' as analysis;

SELECT 
    MARKET_STATUS,
    COUNT(*) as num_markets,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    SUM(TOTAL_CONTRACTS) as total_contracts
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY
GROUP BY MARKET_STATUS
ORDER BY num_markets DESC;

-- =====================================================
-- SECTION 8: SUMMARY INSIGHTS
-- =====================================================

SELECT '========================================' as section;
SELECT '   SECTION 8: KEY INSIGHTS' as section;
SELECT '========================================' as section;

-- Most Active Market Categories (by name patterns)
SELECT 
    'Market Categories by Keywords' as analysis;

SELECT 
    CASE 
        WHEN LOWER(MARKET_NAME) LIKE '%election%' OR LOWER(MARKET_NAME) LIKE '%vote%' THEN 'Elections'
        WHEN LOWER(MARKET_NAME) LIKE '%trump%' THEN 'Trump-related'
        WHEN LOWER(MARKET_NAME) LIKE '%president%' THEN 'Presidential'
        WHEN LOWER(MARKET_NAME) LIKE '%congress%' OR LOWER(MARKET_NAME) LIKE '%senate%' OR LOWER(MARKET_NAME) LIKE '%house%' THEN 'Congress'
        WHEN LOWER(MARKET_NAME) LIKE '%supreme court%' OR LOWER(MARKET_NAME) LIKE '%scotus%' THEN 'Supreme Court'
        WHEN LOWER(MARKET_NAME) LIKE '%economic%' OR LOWER(MARKET_NAME) LIKE '%market%' OR LOWER(MARKET_NAME) LIKE '%stock%' THEN 'Economics'
        WHEN LOWER(MARKET_NAME) LIKE '%world%' OR LOWER(MARKET_NAME) LIKE '%international%' THEN 'International'
        ELSE 'Other'
    END as category,
    COUNT(*) as num_markets,
    SUM(TOTAL_CONTRACTS) as total_contracts,
    ROUND(AVG(TOTAL_CONTRACTS), 2) as avg_contracts_per_market
FROM ETL_DB.ANALYTICS.MARKET_SUMMARY
GROUP BY category
ORDER BY num_markets DESC;

-- Final Summary Message
SELECT '========================================' as section;
SELECT '✅ ANALYSIS COMPLETE!' as message;
SELECT '========================================' as section;
SELECT 
    'Total Markets Analyzed: ' || COUNT(DISTINCT MARKET_ID) || 
    ' | Total Contracts: ' || COUNT(*) ||
    ' | Avg Price: $' || ROUND(AVG(LAST_TRADE_PRICE), 2) as summary
FROM ETL_DB.ANALYTICS.CONTRACT_DETAILS;