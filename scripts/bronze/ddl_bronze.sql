/*
=============================================================
Bronze Layer – DDL Script
=============================================================
Script Purpose:
    This script creates the raw ingestion table for the Layoffs 
    Data Warehouse project. The table stores data as close to 
    the source (CSV) as possible, with only basic type alignment.

Table: bronze.layoffs_raw
    - Holds uncleaned, raw data from layoffs.csv
    - Minimal transformations: 
        * Convert date column into DATE type
        * Align numeric types for totals and percentages
    - All other values kept as-is for traceability

WARNING:
    This table is an ingestion staging area. 
    DO NOT use directly for reporting or analytics.
=============================================================
*/

USE LayoffsDWH;
GO

-- Drop table if it already exists
IF OBJECT_ID('bronze.layoffs_raw', 'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.layoffs_raw;
END;
GO

-- Create bronze table
CREATE TABLE bronze.layoffs_raw (
    company NVARCHAR(255),
    location NVARCHAR(255),
    industry NVARCHAR(255),
    total_laid_off NVARCHAR(50),
    percentage_laid_off NVARCHAR(50),
    [date] NVARCHAR(50),
    stage NVARCHAR(100),
    country NVARCHAR(100),
    funds_raised_millions NVARCHAR(50)
);
