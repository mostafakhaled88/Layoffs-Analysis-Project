USE LayoffsDWH;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, 
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=======================================';
        PRINT 'Loading Bronze Layer - layoffs_raw';
        PRINT '=======================================';

        -- Step 1: Truncate the table
        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.layoffs_raw';
        TRUNCATE TABLE bronze.layoffs_raw;

        -- Step 2: Bulk Insert from CSV
        PRINT 'Inserting Data into: bronze.layoffs_raw';
        BULK INSERT bronze.layoffs_raw
        FROM 'C:\SQLData\layoffs_dataset\layoffs.csv'
        WITH (
            FIRSTROW = 2,             -- skip header row
            FIELDTERMINATOR = ',',    -- CSV delimiter
            ROWTERMINATOR = '0x0d0a', -- Windows line break
            TABLOCK,
            CODEPAGE = '65001'        -- UTF-8 support
        );

        -- Step 3: Log duration
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------';

        -- Step 4: Batch completion
        SET @batch_end_time = GETDATE();
        PRINT '====================================';
        PRINT 'Loading Bronze Layer Completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '====================================';
    END TRY

    BEGIN CATCH
        PRINT '====================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '====================================';
    END CATCH
END;
GO
