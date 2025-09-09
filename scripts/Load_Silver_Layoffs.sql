/*
===============================================================================
Procedure : dbo.Load_Silver_Layoffs
Author    : Mostafa Khaled Farag
Version   : 1.0
Purpose   : 
    ETL procedure to load cleaned layoffs data from bronze layer into silver layer.
    - Removes duplicates
    - Standardizes text values (trim, fix country names, unify industries)
    - Converts data types safely (TRY_CAST, TRY_CONVERT)
    - Fills missing industry values using self-lookup by company
    - Removes rows with no layoff information
    - Reloads silver table from scratch (truncate + insert)
    
Notes:
    - Silver is a clean analytical layer, bronze remains raw.
    - Safe casting prevents ETL failures from dirty values.
    - Designed for daily/weekly batch refresh.
===============================================================================
*/

CREATE OR ALTER PROCEDURE dbo.Load_Silver_Layoffs
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME = GETDATE();

    PRINT '>>> Load_Silver_Layoffs started at ' + CONVERT(VARCHAR, @StartTime, 120);

    /* 1. Create Silver Table if not exists */
    IF OBJECT_ID('silver.layoffs_cleaned', 'U') IS NULL
    BEGIN
        PRINT 'Creating silver.layoffs_cleaned table...';

        CREATE TABLE silver.layoffs_cleaned (
            company NVARCHAR(255),
            location NVARCHAR(255),
            industry NVARCHAR(255),
            total_laid_off INT,
            percentage_laid_off DECIMAL(5,2),
            [date] DATE,
            stage NVARCHAR(255),
            country NVARCHAR(255),
            funds_raised_millions DECIMAL(18,2)
        );
    END
    ELSE
    BEGIN
        PRINT 'Truncating silver.layoffs_cleaned table...';
        TRUNCATE TABLE silver.layoffs_cleaned;
    END;

    /* 2. Transform bronze data into a clean dataset */
    ;WITH cleaned AS (
        SELECT 
            -- Standardize company and location names
            company  = LTRIM(RTRIM(b.company)),
            location = LTRIM(RTRIM(b.[location])),

            -- Standardize industry names and fill nulls from other rows of same company
            industry = COALESCE(
                           NULLIF(LTRIM(RTRIM(
                               CASE 
                                   WHEN b.industry LIKE 'Crypto%' THEN 'Crypto'
                                   WHEN b.industry = 'NULL' OR LTRIM(RTRIM(b.industry)) = '' THEN NULL
                                   ELSE b.industry
                               END
                           )), ''),
                           (
                               SELECT TOP 1 LTRIM(RTRIM(x.industry))
                               FROM bronze.layoffs_raw x
                               WHERE x.company = b.company 
                                     AND x.industry IS NOT NULL 
                                     AND x.industry <> '' 
                                     AND x.industry <> 'NULL'
                           )
                       ),

            -- Convert numeric fields safely
            total_laid_off = CASE 
                                WHEN b.total_laid_off IS NULL OR b.total_laid_off = 'NULL' OR LTRIM(RTRIM(b.total_laid_off)) = '' 
                                    THEN NULL
                                ELSE TRY_CAST(b.total_laid_off AS INT)
                             END,

            percentage_laid_off = CASE 
                                     WHEN b.percentage_laid_off IS NULL OR b.percentage_laid_off = 'NULL' OR LTRIM(RTRIM(b.percentage_laid_off)) = '' 
                                         THEN NULL
                                     ELSE TRY_CAST(b.percentage_laid_off AS DECIMAL(5,2))
                                  END,

            -- Convert string date into DATE type (US format MM/DD/YYYY)
            [date] = TRY_CONVERT(DATE, b.[date], 101),

            -- Standardize stage
            stage = LTRIM(RTRIM(b.stage)),

            -- Fix country formatting (remove trailing dot in US, handle NULLs/blanks)
            country = CASE 
                         WHEN b.country LIKE 'United States%' THEN REPLACE(RTRIM(b.country), '.', '')
                         WHEN b.country = 'NULL' OR LTRIM(RTRIM(b.country)) = '' THEN NULL
                         ELSE LTRIM(RTRIM(b.country))
                      END,

            -- Convert funds safely
            funds_raised_millions = CASE 
                                       WHEN b.funds_raised_millions IS NULL OR b.funds_raised_millions = 'NULL' OR LTRIM(RTRIM(b.funds_raised_millions)) = '' 
                                           THEN NULL
                                       ELSE TRY_CAST(b.funds_raised_millions AS DECIMAL(18,2))
                                    END,

            -- Deduplication: partition by business keys (excluding raw industry to collapse filled ones)
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(b.company)), LTRIM(RTRIM(b.[location])), 
                             b.total_laid_off, b.percentage_laid_off, b.[date], 
                             LTRIM(RTRIM(b.stage)), LTRIM(RTRIM(b.country)), b.funds_raised_millions
                ORDER BY b.company
            ) AS row_num
        FROM bronze.layoffs_raw b
    )

    /* 3. Insert clean records into silver */
    INSERT INTO silver.layoffs_cleaned
    SELECT 
        company, location, industry, total_laid_off, percentage_laid_off,
        [date], stage, country, funds_raised_millions
    FROM cleaned
    WHERE row_num = 1  -- keep only one record per duplicate group
      AND NOT (total_laid_off IS NULL AND percentage_laid_off IS NULL); -- remove useless rows

    DECLARE @EndTime DATETIME = GETDATE();
    PRINT '>>> Load_Silver_Layoffs completed at ' + CONVERT(VARCHAR, @EndTime, 120);
    PRINT '>>> Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) + ' seconds';
END;
GO
