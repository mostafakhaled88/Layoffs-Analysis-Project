/*
===============================================================================
View: vw_Layoffs_Dashboard
===============================================================================
Purpose:
    Provide a clean, analytics-ready dataset for Power BI dashboard reporting.
    Summarizes layoffs data by company, country, industry, and time.

Key Metrics:
    - Total_Laid_Off
    - Avg_Percentage
    - Total_Funds_Raised
===============================================================================
Usage Example:
    SELECT * FROM vw_Layoffs_Dashboard;
===============================================================================
*/

USE LayoffsDB;
GO

IF OBJECT_ID('vw_Layoffs_Dashboard', 'V') IS NOT NULL
    DROP VIEW vw_Layoffs_Dashboard;
GO

CREATE VIEW vw_Layoffs_Dashboard AS
SELECT
    Company,
    Country,
    Location_HQ,
    Industry,
    Stage,
    CAST(Date AS DATE) AS Layoff_Date,
    -- Aggregations
    SUM(Laid_Off_Count) AS Total_Laid_Off,
    AVG(Percentage) AS Avg_Percentage,
    SUM(Funds_Raised) AS Total_Funds_Raised

FROM Layoffs_Clean
GROUP BY
    Company,
    Country,
    Location_HQ,
    Industry,
    Stage,
    CAST(Date AS DATE)
GO
