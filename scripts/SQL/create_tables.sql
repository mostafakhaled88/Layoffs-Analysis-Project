/*
================================================================================
🏗️  Script: create_tables.sql
================================================================================
📘 Purpose:
    - Create the base tables for the Layoffs Analysis Data Warehouse project.
    - This script initializes the staging (raw) and cleaned layers.

📊 Data Source:
    Layoffs.fyi (Kaggle Version)

⚠️ Warning:
    Running this script will DROP existing Layoffs tables and recreate them.
    All existing data in these tables will be deleted.
================================================================================
*/

USE LayoffsDB;
GO

/*==============================================================================
    1️⃣ Drop Existing Tables (if they exist)
==============================================================================*/
IF OBJECT_ID('dbo.LayoffsRaw', 'U') IS NOT NULL
    DROP TABLE dbo.Layoffs_Raw;
GO

IF OBJECT_ID('dbo.Layoffs_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.Layoffs_Clean;
GO


/*==============================================================================
    2️⃣ Create Table: dbo.LayoffsRaw
------------------------------------------------------------------------------
📦 Purpose:
    - Stores the raw layoffs data imported from Python (cleaned CSV).
    - This table mirrors the source structure with minimal transformation.
==============================================================================*/
CREATE TABLE dbo.Layoffs_Raw (
    Company NVARCHAR(100),
    Location_HQ NVARCHAR(100),
    Industry NVARCHAR(100),
    Laid_Off_Count INT NULL,
    [Date] DATE NULL,
    Source NVARCHAR(500),
    Funds_Raised FLOAT NULL,
    Stage NVARCHAR(50),
    Date_Added DATETIME NULL,
    Country NVARCHAR(100),
    Percentage FLOAT NULL,
    List_of_Employees_Laid_Off NVARCHAR(255)
);
GO


/*==============================================================================
    3️⃣ Create Table: dbo.Layoffs_Clean
------------------------------------------------------------------------------
📦 Purpose:
    - Holds the cleaned, standardized, and filtered version of the layoffs data.
    - Populated using the stored procedure: [dbo].[load_Layoffs_Clean].
    - Serves as the foundation for analytics and dashboard views.
==============================================================================*/
CREATE TABLE dbo.Layoffs_Clean (
    Company NVARCHAR(100),
    Country NVARCHAR(100),
    Location_HQ NVARCHAR(100),
    Industry NVARCHAR(100),
    Stage NVARCHAR(50),
    [Date] DATE,
    Laid_Off_Count INT,
    Percentage FLOAT,
    Funds_Raised FLOAT
);
GO


/*==============================================================================
    ✅ Summary
------------------------------------------------------------------------------
    - Created dbo.LayoffsRaw  (Raw Layer)
    - Created dbo.Layoffs_Clean (Clean Layer)
    - Ready for data ingestion and transformation
==============================================================================*/
PRINT '✅ Layoffs Raw and Clean tables created successfully!';
GO
