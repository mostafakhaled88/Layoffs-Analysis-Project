/*
===============================================================================
File: quality_checks_bronze.sql
===============================================================================
Purpose:
    Perform validation checks on the Bronze Layer tables to ensure
    completeness and schema alignment with the source files.

Scope:
    - Validate row count (completeness)
    - Validate schema (expected columns & data types)
    - Validate nulls/blanks in key fields
    - Preview sample data for manual inspection

Usage:
    Run after loading Bronze Layer:
        EXEC bronze.load_bronze;
        :r .\tests\quality_checks_bronze.sql
===============================================================================
*/

USE LayoffsDWH;
GO

PRINT '=======================================================';
PRINT 'Quality Checks - Bronze Layer (layoffs_raw)';
PRINT '=======================================================';

-- 1. Row Count Validation
PRINT '--- Row Count Validation ---';
SELECT COUNT(*) AS row_count
FROM bronze.layoffs_raw;

-- 2. Schema Validation
PRINT '--- Schema Validation (Columns & Data Types) ---';
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'bronze'
  AND TABLE_NAME = 'layoffs_raw';

-- 3. Null & Blank Checks on Critical Columns
PRINT '--- Null/Blank Checks ---';
SELECT 
    SUM(CASE WHEN company IS NULL OR company = '' THEN 1 ELSE 0 END) AS null_companies,
    SUM(CASE WHEN location IS NULL OR location = '' THEN 1 ELSE 0 END) AS null_locations,
    SUM(CASE WHEN industry IS NULL OR industry = '' THEN 1 ELSE 0 END) AS null_industries,
    SUM(CASE WHEN [date] IS NULL OR [date] = '' THEN 1 ELSE 0 END) AS null_dates
FROM bronze.layoffs_raw;

-- 4. Sample Preview
PRINT '--- Sample Data Preview ---';
SELECT TOP 10 *
FROM bronze.layoffs_raw;

PRINT '=======================================================';
PRINT 'Bronze Quality Checks Completed';
PRINT '=======================================================';
GO
