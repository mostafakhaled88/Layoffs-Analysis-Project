/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'LayoffsDB' after checking if it already exists. 
    If the database exists, it is dropped and recreated. 
	
WARNING:
    Running this script will drop the entire 'LayoffsDB' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


USE master;
GO

-- Drop and recreate the 'LayoffsDB' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'LayoffsDB')
BEGIN
    ALTER DATABASE LayoffsDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE LayoffsDB;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE LayoffsDB;
GO

-- Switch to new database
USE SalesDWH;
GO

PRINT ' LayoffsDB initialized ';
