/* 1 - Build an ETL pipeline to load the data from csv files to SQL
Server DB and create a small DataMart according to a specific data model.*/

-- Translate the hybrid architecture into rel. database script
--- OBJECTIVE 
--  THEMES
CREATE TABLE Themes (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(40),
    parent_id INT NULL,
    CONSTRAINT FK_Themes_Parent FOREIGN KEY (parent_id) REFERENCES Themes(id));

-- COLORS
CREATE TABLE Colors (
    id INT NOT NULL PRIMARY KEY,
    name NVARCHAR(200),
    rgb VARCHAR(6),
    is_trans BIT,
    num_parts INT,
    num_sets INT,
    y1 INT,
    y2 INT);

-- PART_CATEGORIES
CREATE TABLE Part_Categories (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(200));

-- PARTS
CREATE TABLE Parts (
    part_num VARCHAR(255) NOT NULL PRIMARY KEY,
    name VARCHAR(250),
    part_cat_id INT NOT NULL,
    part_material VARCHAR(50),
    CONSTRAINT FK_Parts_PartCategories FOREIGN KEY (part_cat_id) REFERENCES Part_Categories(id));

-- PART_RELATIONSHIPS
CREATE TABLE Part_Relationships (
    rel_type VARCHAR(1),
    child_part_num VARCHAR(255) NOT NULL,
    parent_part_num VARCHAR(255) NOT NULL,
    CONSTRAINT FK_PartRelationships_Child FOREIGN KEY (child_part_num) REFERENCES Parts(part_num),
    CONSTRAINT FK_PartRelationships_Parent FOREIGN KEY (parent_part_num) REFERENCES Parts(part_num));

-- ELEMENTS
CREATE TABLE Elements (
    element_id VARCHAR(255) NOT NULL PRIMARY KEY,
    part_num VARCHAR(255) NOT NULL,
    color_id INT NOT NULL,
    design_id INT,
    CONSTRAINT FK_Elements_Parts FOREIGN KEY (part_num) REFERENCES Parts(part_num),
    CONSTRAINT FK_Elements_Colors FOREIGN KEY (color_id) REFERENCES Colors(id));

-- SETS
CREATE TABLE Sets (
    set_num VARCHAR(255) NOT NULL PRIMARY KEY,
    name VARCHAR(256),
    year INT,
    theme_id INT NOT NULL,
    num_parts INT,
    img_url VARCHAR(300),
    CONSTRAINT FK_Sets_Themes FOREIGN KEY (theme_id) REFERENCES Themes(id));

-- MINIFIGS
CREATE TABLE Minifigs (
    fig_num VARCHAR(255) NOT NULL PRIMARY KEY,
    name VARCHAR(256),
    num_parts INT,
    img_url VARCHAR(300));

--- INVENTORIES
CREATE TABLE Inventories (
    id INT NOT NULL PRIMARY KEY,
    version INT,
    set_num VARCHAR(255) NOT NULL,
    CONSTRAINT FK_Inventories_Sets FOREIGN KEY (set_num) REFERENCES Sets(set_num));

---INVENTORY_SETS
CREATE TABLE Inventory_Sets (
    inventory_id INT NOT NULL,
    set_num VARCHAR(255) NOT NULL,
    quantity INT,
    CONSTRAINT FK_Inventory_Sets_Inventories FOREIGN KEY (inventory_id) REFERENCES Inventories(id),
    CONSTRAINT FK_Inventory_Sets_Sets FOREIGN KEY (set_num) REFERENCES Sets(set_num));

--- INVENTORY_PARTS
CREATE TABLE Inventory_Parts (
    inventory_id INT NOT NULL,
    part_num VARCHAR(255) NOT NULL,
    color_id INT NOT NULL,
    quantity INT,
    is_spare BIT,
    img_url VARCHAR(300),
    CONSTRAINT FK_Inventory_Parts_Inventories FOREIGN KEY (inventory_id) REFERENCES Inventories(id),
    CONSTRAINT FK_Inventory_Parts_Parts FOREIGN KEY (part_num) REFERENCES Parts(part_num),
    CONSTRAINT FK_Inventory_Parts_Colors FOREIGN KEY (color_id) REFERENCES Colors(id));

--- INVENTORY_MINIFIGS
CREATE TABLE Inventory_Minifigs (
    inventory_id INT NOT NULL,
    fig_num VARCHAR(255) NOT NULL,
    quantity INT,
    CONSTRAINT FK_Inventory_Minifigs_Inventories FOREIGN KEY (inventory_id) REFERENCES Inventories(id),
    CONSTRAINT FK_Inventory_Minifigs_Minifigs FOREIGN KEY (fig_num) REFERENCES Minifigs(fig_num));

--- 1. a - First METHOD I load from csv and tables are created within chosen database with default data types, not manually but as they are inserted -------------------------------------------------------------
--- What is needed is to add PK and FK constraints to complete the Data Mart Architecture
--- -- another way to import files and create tables at the same time 

create database LEGO2
GO
use LEGO2
GO

select * from Themes where id is null -- default datatypes when inserted 
---
--- Since Columns are being inserted with default data types so they do not exclude any data point, 
--- most of the columns have data types as NULL but PK cannot contain null values so we need to alter data types for PKs.

-- Make ID column NOT NULL before adding the PK
ALTER TABLE Themes ALTER COLUMN id INT NOT NULL;
ALTER TABLE Colors ALTER COLUMN id INT NOT NULL;
ALTER TABLE Part_Categories ALTER COLUMN id INT NOT NULL;
ALTER TABLE Parts ALTER COLUMN part_num VARCHAR(255) NOT NULL;
ALTER TABLE Part_Relationships ALTER COLUMN child_part_num VARCHAR(255) NOT NULL;
ALTER TABLE Part_Relationships ALTER COLUMN parent_part_num VARCHAR(255) NOT NULL;
ALTER TABLE Elements ALTER COLUMN element_id VARCHAR(255) NOT NULL;
ALTER TABLE Sets ALTER COLUMN set_num VARCHAR(255) NOT NULL;
ALTER TABLE Minifigs ALTER COLUMN fig_num VARCHAR(255) NOT NULL;
ALTER TABLE Inventories ALTER COLUMN id INT NOT NULL;
ALTER TABLE Inventory_Sets ALTER COLUMN inventory_id INT NOT NULL;
ALTER TABLE Inventory_Sets ALTER COLUMN set_num VARCHAR(255) NOT NULL;
ALTER TABLE Inventory_Parts ALTER COLUMN inventory_id INT NOT NULL;
ALTER TABLE Inventory_Parts ALTER COLUMN part_num VARCHAR(255) NOT NULL;
ALTER TABLE Inventory_Parts ALTER COLUMN color_id INT NOT NULL;
ALTER TABLE Inventory_Minifigs ALTER COLUMN inventory_id INT NOT NULL;
ALTER TABLE Inventory_Minifigs ALTER COLUMN fig_num VARCHAR(255) NOT NULL;
--- Primary Keys in Parent Tables 
--- Themes
ALTER TABLE Themes ADD CONSTRAINT PK_Themes PRIMARY KEY (id);
---Colors
ALTER TABLE Colors ADD CONSTRAINT PK_Colors PRIMARY KEY (id);
---Part_Categories
ALTER TABLE Part_Categories ADD CONSTRAINT PK_Part_Categories PRIMARY KEY (id);
---Parts
ALTER TABLE Parts ADD CONSTRAINT PK_Parts PRIMARY KEY (part_num);
--- Elements
ALTER TABLE Elements ADD CONSTRAINT PK_Elements PRIMARY KEY (element_id);
--- Sets
ALTER TABLE Sets ADD CONSTRAINT PK_Sets PRIMARY KEY (set_num);
---Minifigs
ALTER TABLE Minifigs ADD CONSTRAINT PK_Minifigs PRIMARY KEY (fig_num);
---Inventories
ALTER TABLE Inventories ADD CONSTRAINT PK_Inventories PRIMARY KEY (id);
---  Inventory_Sets -- one to manyyy 
ALTER TABLE Inventory_Sets ADD CONSTRAINT PK_Inventory_Sets PRIMARY KEY (inventory_id, set_num);
-- Inventory_Minifigs
ALTER TABLE Inventory_Minifigs ADD CONSTRAINT PK_Inventory_Minifigs PRIMARY KEY (inventory_id, fig_num);
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--- Foreign Keys
--- INVENTORIES: One Set (from the Sets table)
--- can appear in many rows in the Inventories table.

SELECT set_num, COUNT(*) AS count_in_inventories
FROM Inventories
GROUP BY set_num
ORDER BY count_in_inventories DESC;
--- one-to-many relationship from Sets to Inventories

--- Make sure that both data types in both keys are the same 
--- Principle related to referential integrity
--- As we haven't had control over the data types as they were being inserted by default
ALTER TABLE Inventories
ALTER COLUMN set_num VARCHAR(255) NOT NULL;

--- Adding foreign key constraint
ALTER TABLE Inventories
ADD CONSTRAINT FK_Inventories_Sets
FOREIGN KEY (set_num) REFERENCES Sets(set_num);

--- INVENTORY_SETS:
--- An inventory may include multiple sets (One to Many)
SELECT inventory_id, COUNT(*) AS usage_count
FROM Inventory_Sets
GROUP BY inventory_id
ORDER BY usage_count DESC;

ALTER TABLE Inventory_Sets
ADD CONSTRAINT FK_Inventory_Sets_Inventories
FOREIGN KEY (inventory_id) REFERENCES Inventories(id);
-------------------
ALTER TABLE Inventory_Sets
ADD CONSTRAINT FK_Inventory_Sets_Sets
FOREIGN KEY (set_num) REFERENCES Sets(set_num);

--- INVENTORY_PARTS (One to Many)
ALTER TABLE Inventory_Parts
ADD CONSTRAINT FK_Inventory_Parts_Inventories
FOREIGN KEY (inventory_id) REFERENCES Inventories(id);
-------------------
ALTER TABLE Inventory_Parts
ADD CONSTRAINT FK_Inventory_Parts_Parts
FOREIGN KEY (part_num) REFERENCES Parts(part_num);
-------------------
ALTER TABLE Inventory_Parts
ADD CONSTRAINT FK_Inventory_Parts_Colors
FOREIGN KEY (color_id) REFERENCES Colors(id);

--- INVENTORY_MINIFIGS (One to Many)
ALTER TABLE Inventory_Minifigs
ADD CONSTRAINT FK_Inventory_Minifigs_Inventories
FOREIGN KEY (inventory_id) REFERENCES Inventories(id);
-------------------
ALTER TABLE Inventory_Minifigs
ADD CONSTRAINT FK_Inventory_Minifigs_Minifigs
FOREIGN KEY (fig_num) REFERENCES Minifigs(fig_num);

-- PARTS

ALTER TABLE Parts
ALTER COLUMN part_cat_id int NOT NULL;

ALTER TABLE Parts
ADD CONSTRAINT FK_Parts_PartCategories
FOREIGN KEY (part_cat_id) REFERENCES Part_Categories(id);

-- PART_RELATIONSHIPS (One to many)
ALTER TABLE Part_Relationships
ADD CONSTRAINT FK_PartRelationships_Child
FOREIGN KEY (child_part_num) REFERENCES Parts(part_num);
-------------------
ALTER TABLE Part_Relationships
ADD CONSTRAINT FK_PartRelationships_Parent
FOREIGN KEY (parent_part_num) REFERENCES Parts(part_num);

-- ELEMENTS

ALTER TABLE Elements
ALTER COLUMN part_num varchar(255) NOT NULL;

ALTER TABLE Elements
ADD CONSTRAINT FK_Elements_Parts
FOREIGN KEY (part_num) REFERENCES Parts(part_num);
-------------------
ALTER TABLE Elements
ALTER COLUMN color_id int NOT NULL;

ALTER TABLE Elements
ADD CONSTRAINT FK_Elements_Colors
FOREIGN KEY (color_id) REFERENCES Colors(id);

--- SETS

ALTER TABLE Sets
ALTER COLUMN theme_id int NOT NULL;
-------------------
ALTER TABLE Sets
ADD CONSTRAINT FK_Sets_Themes
FOREIGN KEY (theme_id) REFERENCES Themes(id);

---- End of Method I Data Has been loaded and database architecture logic has been reflected--------------------------------------------------------------------------------------------------------------
---- 1. b - METHOD II--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--- 1. Manual Creation of Tables and their PK where present
--- Since CSV files have been cleaned// null values have been replaced with 0 and categorical with mode,
--- I can be sure to add data types as NOT NULL in  each column
--- 2. Load of data using python script finding tables with the same name as csv files
--- 3. Adding FK constraints from Parent Tables 
create database LEGO_Rebrickable
go
use LEGO_Rebrickable
GO 

CREATE TABLE Themes (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(100),
    parent_id INT not NULL);
-------------------

CREATE TABLE Colors (
    id INT NOT NULL PRIMARY KEY,
    name NVARCHAR(200),
    rgb VARCHAR(6),
    is_trans BIT,
    num_parts INT,
    num_sets INT,
    y1 INT,
    y2 INT);
-------------------

CREATE TABLE Part_Categories (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(200));
-------------------

CREATE TABLE Parts (
    part_num VARCHAR(255) not NULL PRIMARY KEY,
    name VARCHAR(250),
    part_cat_id INT not NULL,
    part_material VARCHAR(50));
-------------------

CREATE TABLE Part_Relationships (
    rel_type VARCHAR(1),
    child_part_num VARCHAR(255) not NULL,
    parent_part_num VARCHAR(255) not NULL);
-------------------

CREATE TABLE Elements (
    element_id VARCHAR(255) NOT NULL PRIMARY KEY,
    part_num VARCHAR(255)  not NULL,
    color_id INT not NULL,
    design_id INT); --- decided not to delete since it doesn't affect the initial db architecture logic, along with img_url and y1, y2 columns
-------------------
CREATE TABLE Sets (
    set_num VARCHAR(255) not NULL PRIMARY KEY,
    name VARCHAR(256),
    year INT,
    theme_id INT not NULL,
    num_parts INT,
    img_url VARCHAR(300));
-------------------

CREATE TABLE Minifigs (
    fig_num VARCHAR(255) NOT NULL PRIMARY KEY,
    name VARCHAR(256),
    num_parts INT,
    img_url VARCHAR(300));
-------------------

CREATE TABLE Inventories (
    id INT NOT NULL PRIMARY KEY,
    version INT,
    set_num VARCHAR(255) not NULL);
-------------------

CREATE TABLE Inventory_Sets (
    inventory_id INT not NULL,
    set_num VARCHAR(255) not NULL,
    quantity INT);
-------------------

CREATE TABLE Inventory_Parts (
    inventory_id INT not NULL,
    part_num VARCHAR(20) not NULL,
    color_id INT not NULL,
    quantity INT not null,
    is_spare bit not null,
    img_url VARCHAR(500) not null);
-------------------

CREATE TABLE Inventory_Minifigs (
    inventory_id INT not NULL,
    fig_num VARCHAR(255) not NULL,
    quantity INT);
-------------------

select * from themes
--
--- foreign keys
use LEGO14
select * from [dbo].[Inventory_Minifigs]
SELECT DISTINCT i.set_num
FROM Inventories i
LEFT JOIN Sets s ON i.set_num = s.set_num
WHERE s.set_num IS NULL;
--- Realised inconsistency when adding foreign key 
--- This is a foreign key violation scenario
--— The set_num in Inventories points to a value that doesn't exist in the Sets table.
/*DELETE FROM Inventories
WHERE set_num IN (
    SELECT DISTINCT i.set_num
    FROM Inventories i
    LEFT JOIN Sets s ON i.set_num = s.set_num
    WHERE s.set_num IS NULL); ---- optional */

--- Themes self-reference
ALTER TABLE Themes ADD CONSTRAINT FK_Themes_Parent FOREIGN KEY (id) REFERENCES Themes(id);

--- Parts → Part_Categories
ALTER TABLE Parts ADD CONSTRAINT FK_Parts_PartCategories FOREIGN KEY (part_cat_id) REFERENCES Part_Categories(id);

-- Part_Relationships → Parts
ALTER TABLE Part_Relationships ADD CONSTRAINT FK_PartRelationships_Child FOREIGN KEY (child_part_num) REFERENCES Parts(part_num);

ALTER TABLE Part_Relationships ADD CONSTRAINT FK_PartRelationships_Parent FOREIGN KEY (parent_part_num) REFERENCES Parts(part_num);

-- Elements → Parts, Colors
ALTER TABLE Elements
ADD CONSTRAINT FK_Elements_Parts FOREIGN KEY (part_num) REFERENCES Parts(part_num);

ALTER TABLE Elements
ADD CONSTRAINT FK_Elements_Colors FOREIGN KEY (color_id) REFERENCES Colors(id);

-- Sets → Themes
ALTER TABLE Sets
ADD CONSTRAINT FK_Sets_Themes FOREIGN KEY (theme_id) REFERENCES Themes(id);

-- Inventories → Sets -
/*ALTER TABLE Inventories
ADD CONSTRAINT FK_Inventories_Sets FOREIGN KEY (set_num) REFERENCES Sets(set_num);
*/
-- Inventory_Sets → Inventories, Sets
ALTER TABLE Inventory_Sets
ADD CONSTRAINT FK_Inventory_Sets_Inventories FOREIGN KEY (inventory_id) REFERENCES Inventories(id);

ALTER TABLE Inventory_Sets
ADD CONSTRAINT FK_Inventory_Sets_Sets FOREIGN KEY (set_num) REFERENCES Sets(set_num);

-- Inventory_Parts → Inventories, Parts, Colors
ALTER TABLE Inventory_Parts
ADD CONSTRAINT FK_Inventory_Parts_Inventories FOREIGN KEY (inventory_id) REFERENCES Inventories(id);

ALTER TABLE Inventory_Parts
ADD CONSTRAINT FK_Inventory_Parts_Parts FOREIGN KEY (part_num) REFERENCES Parts(part_num);

ALTER TABLE Inventory_Parts
ADD CONSTRAINT FK_Inventory_Parts_Colors FOREIGN KEY (color_id) REFERENCES Colors(id);

-- Inventory_Minifigs → Inventories, Minifigs
ALTER TABLE Inventory_Minifigs
ADD CONSTRAINT FK_Inventory_Minifigs_Inventories FOREIGN KEY (inventory_id) REFERENCES Inventories(id);

ALTER TABLE Inventory_Minifigs
ADD CONSTRAINT FK_Inventory_Minifigs_Minifigs FOREIGN KEY (fig_num) REFERENCES Minifigs(fig_num);
------ End of METHOD 2------------- Data Loaded into created Tables initially and then added the FK constraints to reflect the schema's logic-------------------------------------------------------------

/* 2- 
--- SMALL DATABASE to make the difference between Dimensional and Fact Table*/ 
create database Lego_Assembly_DWH
USE Lego_Assembly_DWH

-- Create the Color dimension table
CREATE TABLE Dim_Color (
    ID INT PRIMARY KEY, 
    Name VARCHAR(200),
    RGB VARCHAR(6),
    Is_Trans BIT );

-- Create the Category dimension table
CREATE TABLE Dim_Category (
    ID INT PRIMARY KEY ,  
    Name VARCHAR(200));

-- Create the Parts fact table with foreign keys to dimension tables
CREATE TABLE Fact_Parts (
    ID INT PRIMARY KEY IDENTITY(1,1),
    Part_Num VARCHAR(20),
    Part_Name VARCHAR(250),
    Color_ID INT,
    Category_ID INT,
    Nr_of_Parts INT,
    Nr_of_Children INT,
    FOREIGN KEY (Color_ID) REFERENCES Dim_Color(ID),
    FOREIGN KEY (Category_ID) REFERENCES Dim_Category(ID));

--- Load Dim_Color from source colors table

INSERT INTO Dim_Color (ID, Name, RGB, Is_Trans)
SELECT id, name, rgb, is_trans FROM LEGO2.[dbo].[colors];

-- Load Dim_Category from source part_categories table

INSERT INTO Dim_Category (ID, Name)
SELECT id, name FROM LEGO2.[dbo].[part_categories]

--- ETL for loading the Fact_Parts table
INSERT INTO Fact_Parts (Part_Num, Part_Name, Color_ID, Category_ID, Nr_of_Parts, Nr_of_Children)
SELECT 
    p.part_num AS Part_Num,
    p.name AS Part_Name,
    e.color_id AS Color_ID,
    p.part_cat_id AS Category_ID,
 
    COALESCE((
        SELECT SUM(ip.quantity) 
        FROM LEGO2.[dbo].[inventory_parts] ip 
        WHERE ip.part_num = p.part_num
    ), 0) AS Nr_of_Parts,
    COALESCE((
        SELECT COUNT(pr.child_part_num) 
        FROM LEGO2.[dbo].[part_relationships] pr 
        WHERE pr.parent_part_num = p.part_num
    ), 0) AS Nr_of_Children FROM 
    LEGO2.[dbo].[parts] p LEFT JOIN 
    LEGO2.[dbo].[elements] e ON p.part_num = e.part_num GROUP BY 
    p.part_num, p.name, e.color_id, p.part_cat_id;
--------------------

use LEGO2
SELECT * FROM part_categories WHERE NAME = 'Technic Bricks'

/*3 -  Build and schedule a stored procedure
In this step, you are required to build a stored procedure for a simple report. The report will count
how many parts of category ‘Technic Bricks’ are with ‘Dark Bluish Gray’ color. The procedure should
be scheduled to execute daily at 10 am.*/

--- Create the stored procedure
--- START SQL SERVER AGENT AT END OF OBJECT EXPLORER WINDOW
--- Purpose: This procedure generates a report counting distinct Technic Bricks in Dark Bluish Gray color,
--- then logs the results to a reporting table for historical tracking and reference.
CREATE PROCEDURE dbo.sp_TechnicBricksReport
AS
BEGIN
    -- Declare variables needed throughout the procedure
    DECLARE @TechnicBricksCategoryID INT,       -- Will store the category ID from Part_Categories table
            @DarkBluishGrayColorID INT,         -- Will store the color ID from Colors table
            @PartCount INT;                     -- Will store the final count of distinct parts
            
    -- Find Category ID for Technic Bricks from the Part_Categories table
    -- This avoids hardcoding IDs, making the procedure more maintainable if category IDs change
    SELECT @TechnicBricksCategoryID = id
    FROM LEGO2.[dbo].[Part_Categories]
    WHERE name = 'Technic Bricks';
    
    -- Find Color ID for Dark Bluish Gray from the Colors table
    -- Similarly, this avoids hardcoding color IDs for better maintainability
    SELECT @DarkBluishGrayColorID = id
    FROM LEGO2.[dbo].[Colors]
    WHERE name = 'Dark Bluish Gray';
    
    -- Count distinct parts that meet our criteria:
    -- 1. Must be in the Technic Bricks category (using the ID we looked up)
    -- 2. Must be Dark Bluish Gray color (using the ID we looked up)
    -- Note: We're using DISTINCT to avoid counting duplicates
    SELECT @PartCount = COUNT(DISTINCT ip.part_num)
    FROM LEGO2.[dbo].[Inventory_Parts] ip
    JOIN LEGO2.[dbo].[Parts] p ON ip.part_num = p.part_num
    WHERE p.part_cat_id = @TechnicBricksCategoryID
      AND ip.color_id = @DarkBluishGrayColorID;

    -- Insert the results into our ReportLog table for historical tracking
    -- This creates a permanent record of when the report was run and what the count was
    INSERT INTO dbo.ReportLog 
    (ReportName, ReportDate, ResultValue)
    VALUES 
    ('Technic Bricks Dark Bluish Gray Count', 
     GETDATE(),                                 -- Current timestamp 
     @PartCount);                               -- Our calculated count value
    
    -- Print the result for immediate feedback when manually executing the procedure
    -- This is helpful for troubleshooting or ad-hoc execution
    PRINT 'Number of Technic Bricks in Dark Bluish Gray: ' + CAST(@PartCount AS VARCHAR(10));
END
GO

--------------------------------------
-- Create the ReportLog table if it doesn't already exist
-- This table serves as a historical repository for all report executions
-- We check if it exists first to avoid errors on subsequent deployments
-- Deferred execution: SQL Server's stored procedure validation doesn't immediately check table existence when creating the procedure, that's why we can create it later on.
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ReportLog')
CREATE TABLE dbo.ReportLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,        -- Auto-incrementing primary key
    ReportName NVARCHAR(255),                   -- Descriptive name of the report
    ReportDate DATETIME DEFAULT GETDATE(),      -- When the report was executed
    ResultValue INT)                            
GO

--------------------------------------
-- SQL Server Agent Job Setup
-- This section creates a scheduled job to automate the report execution
USE msdb; --- control database for automated tasks and operations in SQL Server
GO

--------------------------------------
-- Delete existing job if it exists to avoid duplicates or conflicts
-- This ensures clean deployment even if the job definition changes
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'Daily_TechnicBricks_Report')
    EXEC msdb.dbo.sp_delete_job @job_name = N'Daily_TechnicBricks_Report';
GO

--------------------------------------
-- Create the job container
EXEC msdb.dbo.sp_add_job
    @job_name = N'Daily_TechnicBricks_Report'; 
GO

--------------------------------------
-- Add the job step that defines what the job actually does
-- In this case, it simply executes our stored procedure
EXEC msdb.dbo.sp_add_jobstep
    @job_name = N'Daily_TechnicBricks_Report',
    @step_name = N'Run Technic Bricks Report',  
    @subsystem = N'TSQL',                       -- Using T-SQL execution type
												-- The command text contains Transact-SQL (T-SQL) statements
												-- These statements should be executed directly by the SQL Server Database Engine
												-- The command will be run in the context of the specified database (or the default database if none is specified)
    @command = N'EXEC dbo.sp_TechnicBricksReport'; 
GO

--------------------------------------
-- Create a schedule that defines when the job runs
-- Here we're setting up a daily schedule at 10:00 AM
EXEC msdb.dbo.sp_add_schedule
    @schedule_name = N'DailyAt10AM',            
    @freq_type = 4,                             -- 4 = Daily frequency, 8 = Weekly 
    @freq_interval = 1,                         -- Every 1 day
    @active_start_time = 100000;                -- 10:00:00 AM (format: HHMMSS)
GO

--------------------------------------
-- Connect the job to the schedule we just created
-- This step associates when to run with what to run
EXEC msdb.dbo.sp_attach_schedule
    @job_name = N'Daily_TechnicBricks_Report',
    @schedule_name = N'DailyAt10AM';
GO

--------------------------------------
-- Specify which server should run this job
-- Here we're attaching it to the local server instance
EXEC msdb.dbo.sp_add_jobserver
    @job_name = N'Daily_TechnicBricks_Report';
GO
--------------------------------------
--- LET'S TEST IT NOW
-- Execute the stored procedure
EXEC dbo.sp_TechnicBricksReport;
-- WHERE IT WILL BE STORED 
-- View the entire report log
SELECT * FROM dbo.ReportLog;

-- View the specific Technic Bricks report
SELECT * 
FROM dbo.ReportLog 
WHERE ReportName = 'Technic Bricks Dark Bluish Gray Count'
ORDER BY ReportDate DESC;

-- Output: Count = 16 
------- //  --------------------------------------