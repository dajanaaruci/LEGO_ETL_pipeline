# LEGO Rebrickable ETL Pipeline

## Overview
This project involves building an ETL (Extract, Transform, Load) pipeline for LEGO Rebrickable data with the following components:

1. **Source Database**: Creating tables from CSV files with proper relationships
2. **Data Warehouse**: Building a small data mart with fact and dimension tables and populating it.
3. **Scheduled Procedure**: Creating and scheduling a stored procedure for reporting

## Architecture

### Source Database Architecture
The source database follows a **Third Normal Form (3NF)** relational database model with these characteristics:
- Proper normalization to minimize redundancy
- Clearly defined primary and foreign key relationships
- Parent-child hierarchical relationships ( Themes with self-referencing parent_id)
- Junction tables for many-to-many relationships (inventory_parts, inventory_sets, inventory_minifigs)
- Referential integrity through foreign key constraints

This represents a typical OLTP (Online Transaction Processing) database architecture for operational data storage rather than analytics.

### Data Warehouse Architecture
The data warehouse follows a **star schema** with:

#### Fact Table
- **Parts_Fact**: Contains measures and references to dimension tables
  - Unique_ID (PK)
  - Part_Num
  - Color_ID (FK)
  - Category_ID (FK)
  - Nr_of_Parts (Measure)
  - Nr_of_Children (Measure)

#### Dimension Tables
- **Dim_Color**:
  - ID (PK)
  - Name
  - RGB
  - Is_Trans

- **Dim_Category**:
  - ID (PK)
  - Name

Together, these form a **two-tier data architecture** with:
1. A normalized operational database (shown in the ERD)
2. A denormalized analytical data mart (the star schema)

## Project Architecture Diagrams

### ETL Pipeline Flow
```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│             │     │             │     │                     │
│  CSV Files  │────▶│  SQL Server │───▶│  Data Warehouse     │
│             │     │  Database   │     │  (Star Schema)      │
└─────────────┘     └─────────────┘     └─────────────────────┘
       │                   │                      │
       │                   │                      │
       ▼                   ▼                      ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│ Extraction  │     │Transformation│    │      Loading        │
└─────────────┘     └─────────────┘     └─────────────────────┘
```

### Star Schema Design
```
                ┌───────────────┐
                │  Dim_Color    │
                │ ┌───────────┐ │
                │ │ ID (PK)   │ │
                │ │ Name      │ │
                │ │ RGB       │ │
                │ │ Is_Trans  │ │
                │ └───────────┘ │
                └───────┬───────┘
                        │
                        │
┌───────────────┐       │       ┌───────────────┐
│ Dim_Category  │       │       │  Parts_Fact   │
│ ┌───────────┐ │       │       │ ┌───────────┐ │
│ │ ID (PK)   │ │◄──────┼───────┤ │ Unique_ID │ │
│ │ Name      │ │       │       │ │ Part_Num  │ │
│ └───────────┘ │       └──────►│ │ Color_ID  │ │
└───────────────┘               │ │Category_ID│ │
                                │ │Nr_of_Parts│ │
                                │ │Nr_of_Child│ │
                                │ └───────────┘ │
                                └───────────────┘
```

## About LEGO Rebrickable Data
The Rebrickable dataset contains comprehensive information about LEGO sets, parts, and their relationships. It includes 12 CSV files with data on colors, parts, sets, themes, minifigures, and inventories.

### Dataset Download
All required datasets can be downloaded from the official Rebrickable website:
- **URL**: https://rebrickable.com/downloads/
- **Files needed**: 
  - Colors
  - Elements
  - Inventories
  - Inventory_parts
  - Inventory_sets
  - Inventory_minifigs
  - Minifigs
  - Part_categories
  - Part_relationships
  - Parts
  - Sets
  - Themes

## Project Files
This project includes the following file types:

1. **T-SQL Scripts**:
   - Database schema creation with constraints and referential integrity rules
   - Foreign key relationship definitions
   - Stored procedure implementation
   - SQL Agent job configuration

2. **Jupyter Notebooks**:
   - Data cleaning scripts to handle null values without affecting data types
   - ETL two approaches: extraction, transformation and loading logic
   - Python code for database interaction

3. **ETL Loading Approaches Used**:
   - Direct Approach: Combined table creation and data insertion scripts
   - Phased Approach: Separate scripts for table creation, data insertion, and constraint addition

4. **Input/Output Files**:
   - Input: 12 CSV files from Rebrickable
   - Output: Database backup files for both source database and data warehouse
   - Report results from the stored procedure execution
   - Backup of both databases being created locally

These files collectively implement the complete ETL pipeline from source data to analytical reporting.

## Differences Between Fact and Dimension Tables
- **Fact Tables**: Contain measurable, quantitative data (like counts and sums) and foreign keys to dimension tables. In this project, Parts_Fact contains measures like Nr_of_Parts and Nr_of_Children.

- **Dimension Tables**: Contain descriptive attributes used for filtering and grouping. Here, Dim_Color and Dim_Category provide contextual information for the measures in the fact table.

## Stored Procedure and SQL Agent Job
A stored procedure has been created to generate a report counting how many parts of category 'Technic Bricks' are with 'Dark Bluish Gray' color. This procedure is scheduled to run daily at 10:00 AM using SQL Server Agent.
The SQL Agent job ensures automated execution of the report and can be monitored through SQL Server Management Studio.
