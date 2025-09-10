/*
===============================================================================
Procedure  : dbo.Load_Gold_Layoffs
Author     : Mostafa Khaled Farag
Purpose    : Incrementally load the Gold Layer from Silver Layer
             - Load Dimensions: dim_company, dim_location (with region), dim_industry, dim_date
             - Load Fact: fact_layoffs with foreign keys
             - Prevent duplicates using NOT EXISTS
Dependencies:
    - silver.layoffs_cleaned
    - gold.dim_company, gold.dim_location, gold.dim_industry, gold.dim_date
===============================================================================
*/

CREATE OR ALTER PROCEDURE dbo.Load_Gold_Layoffs
AS
BEGIN
    SET NOCOUNT ON;  -- Avoid extra messages for inserts/updates

    -----------------------------------------------------------------
    -- 1. Ensure gold schema exists
    -----------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
        EXEC('CREATE SCHEMA gold');

    -----------------------------------------------------------------
    -- 2. Create Dimensions if they do not exist
    -----------------------------------------------------------------
    -- Company dimension
    IF OBJECT_ID('gold.dim_company', 'U') IS NULL
    BEGIN
        CREATE TABLE gold.dim_company (
            company_id INT IDENTITY(1,1) PRIMARY KEY,
            company_name NVARCHAR(255) NOT NULL,
            stage NVARCHAR(255) NULL
        );
    END;

    -- Location dimension
    IF OBJECT_ID('gold.dim_location', 'U') IS NULL
    BEGIN
        CREATE TABLE gold.dim_location (
            location_id INT IDENTITY(1,1) PRIMARY KEY,
            location NVARCHAR(255) NOT NULL,
            country NVARCHAR(255) NOT NULL,
            region NVARCHAR(255) NULL
        );
    END;

    -- Industry dimension
    IF OBJECT_ID('gold.dim_industry', 'U') IS NULL
    BEGIN
        CREATE TABLE gold.dim_industry (
            industry_id INT IDENTITY(1,1) PRIMARY KEY,
            industry_name NVARCHAR(255) NOT NULL,
            sector NVARCHAR(255) NULL
        );
    END;

    -- Date dimension
    IF OBJECT_ID('gold.dim_date', 'U') IS NULL
    BEGIN
        CREATE TABLE gold.dim_date (
            date_id INT IDENTITY(1,1) PRIMARY KEY,
            full_date DATE NOT NULL,
            year INT,
            quarter INT,
            month INT,
            month_name NVARCHAR(20),
            week INT,
            day INT
        );
    END;

    -----------------------------------------------------------------
    -- 3. Create Fact table if it does not exist
    -----------------------------------------------------------------
    IF OBJECT_ID('gold.fact_layoffs', 'U') IS NULL
    BEGIN
        CREATE TABLE gold.fact_layoffs (
            fact_id INT IDENTITY(1,1) PRIMARY KEY,
            company_id INT,
            location_id INT,
            industry_id INT,
            date_id INT,
            total_laid_off INT,
            percentage_laid_off DECIMAL(5,2),
            funds_raised_millions DECIMAL(18,2),
            CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES gold.dim_company(company_id),
            CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES gold.dim_location(location_id),
            CONSTRAINT fk_industry FOREIGN KEY (industry_id) REFERENCES gold.dim_industry(industry_id),
            CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES gold.dim_date(date_id)
        );
    END;

    -----------------------------------------------------------------
    -- 4. Load Dimensions Incrementally
    -----------------------------------------------------------------
    -- 4.1 Company
    INSERT INTO gold.dim_company (company_name, stage)
    SELECT DISTINCT s.company, s.stage
    FROM silver.layoffs_cleaned s
    WHERE s.company IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 
            FROM gold.dim_company c
            WHERE c.company_name = s.company 
              AND c.stage = s.stage
      );

    -- 4.2 Location with region mapping
    INSERT INTO gold.dim_location (location, country, region)
    SELECT DISTINCT
        s.location,
        s.country,
        CASE 
            WHEN s.country IN ('United States','Canada','Mexico','Brazil','Argentina','Chile','Colombia') THEN 'Americas'
            WHEN s.country IN ('United Kingdom','Germany','France','Netherlands','Sweden','Norway','Finland','Ireland','Spain','Portugal','Italy','Denmark','Austria','Poland','Romania','Russia','Estonia','Lithuania','Luxembourg','Bulgaria','Switzerland','Hungary') THEN 'Europe'
            WHEN s.country IN ('United Arab Emirates','Israel','Turkey','Egypt','Nigeria','South Africa','Kenya','Senegal','Seychelles') THEN 'Middle East & Africa'
            WHEN s.country IN ('India','Singapore','Japan','China','Hong Kong','Malaysia','Indonesia','Thailand','Vietnam','Myanmar','Pakistan','South Korea','Australia','New Zealand') THEN 'Asia-Pacific'
            ELSE 'Other'
        END AS region
    FROM silver.layoffs_cleaned s
    WHERE s.location IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 
            FROM gold.dim_location l
            WHERE l.location = s.location
              AND l.country = s.country
      );

    -- 4.3 Industry
    INSERT INTO gold.dim_industry (industry_name)
    SELECT DISTINCT s.industry
    FROM silver.layoffs_cleaned s
    WHERE s.industry IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 
            FROM gold.dim_industry i
            WHERE i.industry_name = s.industry
      );

    -- 4.4 Date
    INSERT INTO gold.dim_date (full_date, year, quarter, month, month_name, week, day)
    SELECT DISTINCT
        s.[date],
        YEAR(s.[date]),
        DATEPART(QUARTER, s.[date]),
        MONTH(s.[date]),
        DATENAME(MONTH, s.[date]),
        DATEPART(WEEK, s.[date]),
        DAY(s.[date])
    FROM silver.layoffs_cleaned s
    WHERE s.[date] IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 
            FROM gold.dim_date d
            WHERE d.full_date = s.[date]
      );

    -----------------------------------------------------------------
    -- 5. Load Fact table incrementally
    -----------------------------------------------------------------
    INSERT INTO gold.fact_layoffs (
        company_id, location_id, industry_id, date_id,
        total_laid_off, percentage_laid_off, funds_raised_millions
    )
    SELECT 
        c.company_id,
        l.location_id,
        i.industry_id,
        d.date_id,
        s.total_laid_off,
        s.percentage_laid_off,
        s.funds_raised_millions
    FROM silver.layoffs_cleaned s
    LEFT JOIN gold.dim_company c ON s.company = c.company_name AND s.stage = c.stage
    LEFT JOIN gold.dim_location l ON s.location = l.location AND s.country = l.country
    LEFT JOIN gold.dim_industry i ON s.industry = i.industry_name
    LEFT JOIN gold.dim_date d ON s.[date] = d.full_date
    WHERE NOT EXISTS (
        SELECT 1 
        FROM gold.fact_layoffs f
        WHERE f.company_id = c.company_id
          AND f.location_id = l.location_id
          AND f.industry_id = i.industry_id
          AND f.date_id = d.date_id
          AND ISNULL(f.total_laid_off, -1) = ISNULL(s.total_laid_off, -1)
          AND ISNULL(f.percentage_laid_off, -1) = ISNULL(s.percentage_laid_off, -1)
          AND ISNULL(f.funds_raised_millions, -1) = ISNULL(s.funds_raised_millions, -1)
    );

    -----------------------------------------------------------------
    -- 6. Completion message
    -----------------------------------------------------------------
    PRINT 'Gold Layer Incremental Load Completed at ' + CONVERT(VARCHAR, GETDATE(), 120);

END;
GO
