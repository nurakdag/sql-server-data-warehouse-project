/*This script drops the existing 'DataWareHouse' database if it exists and creates a new empty 'DataWareHouse' database. And create three schema within the database : "bronze", "silver", "gold"  */


USE master;
GO

-- Drop and recreate the "DataWareHouse" database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
    ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWareHouse;
END;
GO

-- Create the "DataWareHouse" database
CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

--Create Schemas
Create SCHEMA bronze;
GO

Create SCHEMA silver;

Create SCHEMA gold;
