/*
===============================================================================
Stored Procedure: load_Layoffs_Clean
===============================================================================
Purpose:
    Load data from the Layoffs_Raw (raw data) into the Layoffs_Clean (cleaned, 
    structured, and conformed table). The process includes:
    - Truncating existing Layoffs_Clean table.
    - Cleaning and transforming data (standardizing industries).
    - Counting number of rows inserted.

===============================================================================
Usage Example:
    EXEC load_Layoffs_Clean;
===============================================================================
Change Log:
    2025-10-17 | Mostafa Khaled | Initial version with row count tracking
===============================================================================
*/

USE LayoffsDB;
GO

IF OBJECT_ID('load_Layoffs_Clean', 'P') IS NOT NULL
    DROP PROCEDURE load_Layoffs_Clean;
GO

CREATE PROCEDURE load_Layoffs_Clean
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @rows_inserted INT;

    PRINT '================================================';
    PRINT '⚙️  Starting Layoffs_Clean Load';
    PRINT '================================================';

    BEGIN TRY

        -------------------------------------------------------------------
        -- 1. Truncate Target Table
        -------------------------------------------------------------------
        PRINT '→ Truncating Layoffs_Clean...';
        TRUNCATE TABLE Layoffs_Clean;

        -------------------------------------------------------------------
        -- 2. Load Cleaned Data
        -------------------------------------------------------------------
        PRINT '→ Loading Cleaned Layoffs Data...';

        INSERT INTO Layoffs_Clean
        (
            Company,
            Country,
            Location_HQ,
            Industry,
            Stage,
            Date,
            Laid_Off_Count,
            Percentage,
            Funds_Raised
        )
        SELECT 
            LTRIM(RTRIM(Company)) AS Company,
            LTRIM(RTRIM(Country)) AS Country,
            LTRIM(RTRIM(Location_HQ)) AS Location_HQ,

            CASE
                WHEN Company IN ('Microsoft', 'Oracle', 'SAP', 'Red Hat', 'Viasat', 'Ericsson', 'Sinch', 'Impinj', 'AudioCodes', 'Fluke','Lokalise', 'Lendis','Appsmith') THEN 'Infrastructure'
                WHEN Company IN ('Zoom', 'Twilio', 'RingCentral', 'Hopin', 'Avaya', 'Formstack', 'Smartsheet', 'Miro', 'ClickUp', 'Asana', 'Lucid Software', 'Atlassian', 'Dropbox', 'Autodesk','WalkMe', 'Tonkean', 'Rows','Pitch', 'Calendly') THEN 'Product'
                WHEN Company IN ('Unity', 'Niantic', 'Improbable', 'Singularity 6', 'Element AI') THEN 'AI'
                WHEN Company IN ('Zymergen', 'Indigo', 'Infarm', 'Agility Robotics', 'Desktop Metal', 'Plus One Robotics', 'American Robotics','Electric') THEN 'Manufacturing'
                WHEN Company IN ('Fiverr', 'Upwork', 'Chief', 'TealBook', 'Jimdo') THEN 'Recruiting'
                WHEN Company IN ('Planetly') THEN 'Energy'
                WHEN Company IN ('ForgeRock', 'Unico', 'Gated', 'L1ght', 'Torii') THEN 'Security'
                WHEN Company IN ('Benchling', 'Papa', 'Q4') THEN 'Healthcare'
                WHEN Company IN ('Tackle', 'Netlify', 'Workato', 'Datree') THEN 'Infrastructure'
                WHEN Company IN ('Qualtrics', 'Similarweb', 'Symend', 'MessageBird', 'Zencity') THEN 'Marketing'
                WHEN Company IN ('Catalant', 'Bonterra', 'Benevity', 'Submittable') THEN 'HR'
                WHEN Company IN ('Blackbaud', 'Vendr', 'Consider.co', 'Hubilo') THEN 'Finance'
                WHEN Company IN ('Tekion', 'WeTransfer', 'Weedmaps', 'LeafLink', 'Dutchie', 'View', 'Hyland Software') THEN 'Software'
                WHEN Company IN ('Thoughtworks') THEN 'Consulting'
                WHEN Company IN ('Starry') THEN 'Telecom'
                WHEN Company IN ('Intrinsic', 'Automation Anywhere') THEN 'AI'
                WHEN Company IN ('SoundHound', 'Bandcamp') THEN 'Media'
                WHEN Company IN ('Pico Interactive', 'Fable', 'Meero') THEN 'Consumer'
                WHEN Company IN ('Kinde', 'Slite', 'Kinde') THEN 'Product'
                WHEN Company IN ('ResearchGate', 'MyGate') THEN 'Data'
                WHEN Company IN ('Electric') THEN 'IT Services'
                ELSE CASE WHEN Industry <> 'Other' THEN Industry ELSE 'Other' END
            END AS Industry,

            LTRIM(RTRIM(Stage)) AS Stage,
            Date,
            Laid_Off_Count,
            Percentage,
            Funds_Raised
        FROM Layoffs_Raw
        WHERE 
            Laid_Off_Count > 0 
            AND Percentage > 0;

        -------------------------------------------------------------------
        -- 3. Row Count Tracking
        -------------------------------------------------------------------
        SET @rows_inserted = @@ROWCOUNT;

        -------------------------------------------------------------------
        -- 4. Summary
        -------------------------------------------------------------------
        DECLARE @end_time DATETIME = GETDATE();
        PRINT '================================================';
        PRINT '✅ Load Completed Successfully!';
        PRINT '   - Rows Inserted: ' + CAST(@rows_inserted AS NVARCHAR);
        PRINT '   - Total Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY

    BEGIN CATCH
        PRINT '================================================';
        PRINT '❌ ERROR OCCURRED DURING CLEAN LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
GO
