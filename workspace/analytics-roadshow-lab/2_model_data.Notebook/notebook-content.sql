-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "e0b59438-dd1e-aef5-4826-ec248fd6e79b",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "e0b59438-dd1e-aef5-4826-ec248fd6e79b",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- <div style="margin: 0; padding: 0; text-align: left;">
--   <table style="border: none; margin: 0; padding: 0; border-collapse: collapse;">
--     <tr>
--       <td style="border: none; vertical-align: middle; text-align: left; padding: 0; margin: 0;">
--         <img src="https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/warehouse/fabric-data-warehouse-icon.png?raw=true" width="140" />
--       </td>
--       <td style="border: none; vertical-align: middle; padding-left: 30px; text-align: left; padding-right: 0; padding-top: 0; padding-bottom: 0;">
--         <h1 style="font-weight: bold; margin: 0;">Fabric Analytics Roadshow Lab</h1>
--       </td>
--     </tr>
--   </table>
-- </div>
-- 
-- ## Overview
-- Let's continue the **McMillan Industrial Group** analytics transformation journey! In this lab, you'll use the data collected in the Lakehouse from the previous lab to build a data model for analytics using Fabric Data Warehouse.
-- 
-- ### The Business Scenario
-- McMillan Industrial Group is a leading manufacturer and distributor of industrial equipment and parts. Their systems generate a variety of data. The analytical data model will be focused on:
-- - 👥 **Customers** - Customer master data and profiles
-- - 📝 **Orders** - Sales orders placed online and manually
-- - ⚙️ **Items** - Item master data
-- - 📦 **Shipments** - Outbound shipments and delivery tracking
-- 
-- This data has been collected, cleansed, and conformed into actionable data (silver in the medallion architecture) in a lakehouse.
-- 
-- ### Architecture: Medallion Pattern
-- We'll implement a **medallion architecture** - a common practice for organizing data based on the level of data refinement and readiness for end-user consumption:
-- 
-- ```
-- 📥 Landing Zone (Raw Data: JSON/Parquet)
--     ↓ Spark - Structured Streaming
-- 🥉 BRONZE Zone - Raw ingestion with audit columns and column name cleaning
--     ↓ Spark - Structured Streaming
-- 🥈 SILVER Zone - Cleaned, validated, and conformed data
--     ↓ Fabric Warehouse - Dimensional Modeling
-- 🥇 GOLD Zone - Business-level aggregates (Warehouse)
--     ↓
-- 🤖 Analytics & AI - Data Agent and Semantic Models
-- ```
-- 
-- ---
-- 
-- ## 🎯 Lab Setup
-- 
-- Before we explore data warehouse fundamentals, you need to ensure Lab 1 has been completed.
-- 
-- ### What You'll Learn in This Notebook
-- 
-- 1. **Data warehouse fundamentals** - What are dimensions and facts?
-- 1. **Working with schemas and tables** - Create schemas and explore supported DDL
-- 1. **Loading and transforming data with T-SQL** - Use stored procedures to move data from silver to gold
-- 1. **Beyond the basics** - Monitor the data warehouse and see how we achieve performance by default
-- 1. **Operationalize warehouse loading** - Orchestrate and schedule data warehouse loading with Data Factory
-- 
-- ### The Target Schema
-- By the end of the lab, you'll understand the basics of dimensional modeling and how to implement them using a Fabric data warehouse:
-- 
-- ![McMillian Industrial Group Gold Schema](https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/warehouse/gold-erd.png?raw=true)
-- 
-- Let's get started!


-- MARKDOWN ********************

-- ## 📚 Part 1: Data Warehouse Fundamentals
-- 
-- Before we start creating tables and transforming data, let's explore some core concepts of data warehousing which will set the foundation for what we will build in the rest of this lab.
-- 
-- ### Dimensions
-- - Represent different ways to **slice and dice** business events. Examples include by customer, product, or date.
-- - Each column represents an **attribute** which describe the dimension. For example, the product dimension may contain attributes like size, color, or weight.
-- - Dimensions are often **denormalized** to optimize query performance and ease of use.
-- - To track and analyze changes over time a **slowly changing dimension** can be implemented. The most common are:
--     - **Type 0**: The attribute never changes (Social Security Number, or Birth Date)
--     - **Type 1**: The attribute can change but history is not tracked because the changes lack analytical value (Email addresses or Phone Numbers often fall in this category)
--     - **Type 2**: The attribute can change and the history is tracked because it can add analytical value (Change in a customer's home address could be used to analyze purchasing habits over time)
-- 
-- ### Facts
-- - Represent business events that are numeric and measurable. Examples include sales, inventory, or visits.
-- - Columns represent **measures** at a common grain (detail level) which can be aggregated.
-- - Tables are often relatively narrow and very long.
-- - Additional columns contain keys back to dimensions which describe the events.
-- - Fact tables generally fall into the following categories:
--     - **Transactional**: Each row represents an individual transaction such as an order or a call to a call center.
--     - **Snapshot**: Track periodic views of the data over time each with a common snapshot date such as product inventory.
--     - **Factless Fact** (aka **Bridge**): Do not contain measures, instead just contain a set of intersecting keys. This is commonly used for defining security and can be used to track things like student attendance for a class for a particular date).


-- MARKDOWN ********************

-- ## 🧩 Part 2: Working with Schemas and Tables
-- 
-- In this section we will see how to use schemas for logically grouping tables into dimensions and facts to help users easily understand the data model. We will also create all the tables needed for the dimensional model with the smallest required data types and use capabilities like identity columns for key generation.
-- 
-- **Organizing tables**
-- 
-- Fact and dimension tables will serve as the foundation for the data warehouse's analytical structure. There are many ways to organize these tables. Some organizations will choose to prepen or postpend "Fact" or "Dim" on the table name (DimDate or Date_Dim). Others will choose to use schemas to group all the dimensions and facts (dim.Date and fact.Sale). The method you choose is largely one of preference; just remember to be consistent in the database design. For this lab, we will use schemas.
-- 
-- To get started, let's create two schemas, dim and fact, then verify they have been created successfully. 

-- CELL ********************

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dim')
EXEC ('CREATE SCHEMA [dim]')

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fact')
EXEC ('CREATE SCHEMA [fact]')

SELECT * FROM sys.schemas WHERE name IN ('dim', 'fact')

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- With the schemas created, it's time to create the tables. 
-- 
-- **Column names** 
-- 
-- A data warehouse is designed to be used by a wide variety of users across the organization. Users will often be from different departments or divisions. It is important to use business friendly terminology that everyone has agreed upon in the warehouse columns. For example, a source system column like t_t_amt_1 which shows up in the silver layer may not make sense to a business user. Instead translate this to a friendly name like total_amount_with_tax. Again, the use of underscores, camel case, and snake case are one of preference, just remember to be consistent. For this lab, we will use underscores. 
-- 
-- **Indexes and statistics** 
-- 
-- Even though data warehouses are generally index-lite databases Fabric data warehouse does not require any indexing! Similarly, no user action is required for statistics; they will be maintained automatically. As a result, the scripts that follow in this lab will not drop, create, or update any indexes or statistics. 
-- 
-- **Identity columns**
-- 
-- Identity columns are widely used for create surrogate keys. A surrogate key is a key generated in the data warehouse for joining tables. These are important because many source systems can have different key composition. One system may use an integer, another a VARCHAR(36), and another a composite key with multiple columns. Using a surrogate key makes joining tables easy and efficient. The surrogate key also aids in tracking changes (SCD Type 2) because each version of the record will carry the same business key (also known as an alternate key or AK). In Fabric data warehouse the identity column carries a data type of BIGINT.
-- 
-- **Data types** 
-- 
-- Data type decisions are important as they can have an impact on resource allocation and performance. It is a best practice to always choose the smallest data type necessary. Don't use a BIGINT when an INT will do. Don't use VARCHAR(MAX) when a VARCHAR(50) will hold all the data with a pad for some potential new data. Use integers for key fields when possible. The Fabric warehouse engine does not support the same exact data types found in the SQL Server engine, but they are very close. For example, NVARCHAR is not supported, but because of the collation used on VARCHAR fields there shouldn't be issues with storing any characters. DATETIME2 supports up to a precision of 6 rather than 7. 
-- 
-- For an up to date list of data types refer to the [Data Types in Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types) documentation page. 
-- 
-- The script below will create a total of 8 tables:
-- - Dim Address
-- - Dim Customer
-- - Dim Date
-- - Dim Facility
-- - Dim Item
-- - Dim Order Source
-- - Fact Sale
-- - Fact Shipment


-- CELL ********************

/* Drop the tables if they exist, this is useful in case the script is run multiple times. */
DROP TABLE IF EXISTS [dim].[address]
DROP TABLE IF EXISTS [dim].[customer]
DROP TABLE IF EXISTS [dim].[date]
DROP TABLE IF EXISTS [dim].[facility]
DROP TABLE IF EXISTS [dim].[item]
DROP TABLE IF EXISTS [dim].[order_source]
DROP TABLE IF EXISTS [fact].[order]
DROP TABLE IF EXISTS [fact].[shipment]
GO

/* Create each table. Notice the identity columns on each dimension have a data type of BIGINT. */
CREATE TABLE [dim].[address]
	(
		[address_sk] 		    [bigint] IDENTITY   NOT NULL,
		[address_line_1] 	    [varchar](50)       NOT NULL,
		[address_line_2] 	    [varchar](50)       NOT NULL,
		[city] 				    [varchar](50)       NOT NULL,
		[state_abbreviation]    [varchar](10)       NOT NULL,
		[zip_code] 			    [varchar](10)       NOT NULL,
		[country] 			    [varchar](10)       NOT NULL,
		[latitude] 			    [float]             NOT NULL,
		[longitude] 		    [float]             NOT NULL
	)

CREATE TABLE [dim].[customer]
	(
		[customer_sk] 					[bigint] IDENTITY   NOT NULL,
		[customer_ak] 					[varchar](50) 	    NOT NULL,
		[customer_name] 				[varchar](50) 	    NOT NULL,
		[customer_description] 			[varchar](100) 	    NOT NULL,
		[primary_contact_first_name] 	[varchar](50) 	    NOT NULL,
		[primary_contact_last_name] 	[varchar](50) 	    NOT NULL,
		[primary_contact_email] 		[varchar](50) 	    NOT NULL,
		[primary_contact_phone] 		[varchar](30) 	    NOT NULL,
		[delivery_city] 				[varchar](50) 	    NOT NULL,
		[delivery_country] 				[varchar](10) 	    NOT NULL,
		[delivery_latitude] 			[float] 		    NOT NULL,
		[delivery_longitude] 			[float] 		    NOT NULL,
		[delivery_state] 				[varchar](10) 	    NOT NULL,
		[delivery_zip_code] 			[varchar](10) 	    NOT NULL,
		[billing_city] 					[varchar](50) 	    NOT NULL,
		[billing_country] 				[varchar](10) 	    NOT NULL,
		[billing_state] 				[varchar](10) 	    NOT NULL,
		[start_date] 					[datetime2](6) 	    NOT NULL,
		[end_date] 						[datetime2](6) 	    NOT NULL
	)

CREATE TABLE [dim].[date]
    (
        [date_sk]       [INT]           NOT NULL,
        [date]          [DATE]          NOT NULL,
        [day_number]    [VARCHAR](7)    NOT NULL,
        [day_of_week]   [VARCHAR](9)    NOT NULL,
        [month_number]  [VARCHAR](7)    NOT NULL,
        [month_name]    [VARCHAR](9)    NOT NULL,
        [quarter]       [SMALLINT]      NOT NULL,
        [year]          [SMALLINT]      NOT NULL
    )

INSERT INTO [dim].[date]
VALUES (19000101, '1900-01-01', 'Unknown', 'Unknown', 'Unknown', 'Unknown', -1, -1)

INSERT INTO [dim].[date]
SELECT
    /*  Date  */
    CONVERT(VARCHAR, [date], 112) AS date_sk,
    [date] AS [date],
    FORMAT([date], 'dd') AS day_number,
    FORMAT([date], 'dddd') AS day_of_week,
    FORMAT([date], 'MM') AS month_number,
    FORMAT([date], 'MMMM') AS month_name,
    DATEPART(QUARTER, [date]) AS [quarter],
    DATEPART(YEAR, [date]) AS [year]
FROM
    (
        SELECT
            DATEADD(DAY, [value], CONVERT(DATE, '2026-01-01')) AS [date]
        FROM GENERATE_SERIES(0, DATEDIFF(DAY, CONVERT(DATE, '2026-01-01'), CONVERT(DATE, '2029-01-01')), 1)
    ) AS dates

CREATE TABLE [dim].[facility]
    (
        [facility_sk] 	        [bigint] IDENTITY   NOT NULL,
        [facility_ak] 	        [varchar](10) 	    NOT NULL,
        [facility_name]         [varchar](50) 	    NOT NULL,
        [facility_type]         [varchar](25) 	    NOT NULL,
        [address] 		        [varchar](50) 	    NOT NULL,
        [city] 			        [varchar](50) 	    NOT NULL,
        [state_abbreviation] 	[varchar](10) 	    NOT NULL,
        [zip_code] 		        [varchar](10) 	    NOT NULL,
        [country] 		        [varchar](10) 	    NOT NULL,
        [latitude] 		        [float] 		    NOT NULL,
        [longitude] 	        [float] 		    NOT NULL,
        [start_date]            [datetime2](6) 	    NOT NULL,
		[end_date] 	            [datetime2](6) 	    NOT NULL
    )

CREATE TABLE [dim].[item]
	(
		[item_sk] 			[bigint] IDENTITY   NOT NULL,
		[item_ak] 			[varchar](50) 	    NOT NULL,
		[sku] 				[varchar](25) 	    NOT NULL,
		[item_description] 	[varchar](500) 	    NOT NULL,
		[brand] 			[varchar](50) 	    NOT NULL,
		[category] 			[varchar](50) 	    NOT NULL,
		[subcategory] 		[varchar](50) 	    NOT NULL,
		[material] 			[varchar](50) 	    NOT NULL,
		[nominal_size] 		[float] 		    NOT NULL,
		[end_connection] 	[varchar](25) 	    NOT NULL,
		[pressure_class] 	[bigint] 		    NOT NULL,
		[weight] 			[float] 		    NOT NULL,
		[cost] 				[float] 		    NOT NULL,
		[list_price] 		[float] 		    NOT NULL,
		[is_sdofcertified] 	[bit] 			    NOT NULL,
		[structural_index] 	[float] 		    NOT NULL,
		[span_rating] 		[float] 		    NOT NULL,
        [start_date] 		[datetime2](6) 	    NOT NULL,
		[end_date] 			[datetime2](6) 	    NOT NULL
	)

CREATE TABLE [dim].[order_source]
	(
		[order_source_sk] 	[bigint] IDENTITY   NOT NULL,
		[order_source_ak] 	[varchar](10) 	    NOT NULL,
		[order_source_name] [varchar](10) 	    NOT NULL,
        [start_date] 		[datetime2](6) 	    NOT NULL,
		[end_date] 			[datetime2](6) 	    NOT NULL
	)

CREATE TABLE [fact].[order]
	(
		[order_sk]              [bigint] IDENTITY   NOT NULL,
        [order_number] 			[varchar](25)	    NOT NULL,
		[order_line_number] 	[smallint] 		    NOT NULL,
		[order_date_sk] 		[int] 			    NOT NULL,
		[order_source_sk] 		[bigint]		    NOT NULL,
		[customer_sk] 			[bigint] 		    NOT NULL,
		[item_sk] 				[bigint] 		    NOT NULL,
		[quantity] 				[bigint] 		    NULL,
		[unit_price] 			[float] 		    NULL,
		[extended_price] 		[float] 		    NULL,
		[net_weight] 			[float] 		    NULL,
		[warranty_included] 	[bit] 			    NULL
	)

CREATE TABLE [fact].[shipment]
	(
		[shipment_sk]                       [bigint] IDENTITY   NOT NULL,
        [tracking_number] 					[varchar](25) 	    NOT NULL,
		[order_number] 						[varchar](25) 	    NOT NULL,
		[ship_date_sk] 						[int] 			    NOT NULL,
		[committed_delivery_date_sk] 		[int] 			    NOT NULL,
		[delivery_date_sk] 					[int] 			    NOT NULL,
		[customer_sk] 						[bigint] 		    NOT NULL,
		[origin_address_sk] 				[bigint] 		    NOT NULL,
		[destination_address_sk] 			[bigint] 		    NOT NULL,
		[service_level] 					[varchar](25) 	    NULL,
		[delivery_days_late] 				[int] 			    NULL,
		[late_delivery_penalty_per_day] 	[float] 		    NULL,
		[late_delivery_penalty] 			[float] 		    NULL,
		[shipment_distance] 				[float] 		    NULL,
		[declared_value] 					[float] 		    NULL,
		[height] 							[float] 		    NULL,
		[width] 							[float] 		    NULL,
		[length] 							[float] 		    NULL,
		[volume] 							[float] 		    NULL,
		[weight] 							[float] 		    NULL,
		[is_fragile] 						[bit] 			    NULL,
		[is_hazardous] 						[bit] 			    NULL,
		[requires_refrigeration] 			[bit] 			    NULL
	)
GO

SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('dim', 'fact')

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 🛠️ Part 3: Loading and Transforming Data With T-SQL 
-- 
-- With all the tables created it is time to shift the focus to data transformation and loading. 
-- 
-- Before looking at moving data from the silver to gold layers, let's take a look at a way to bring in data from outside the warehouse using the T-SQL COPY INTO and OPENROWSET commands. 
-- 
-- The **COPY** command is a high performance loading mechanism for bringing data in from Azure storage. In this lab we will bring in a static dataset, the date dimension, as it only needs to be loaded once and there is no additional transformation logic required. 
-- 
-- The **OPENROWSET** command allows you to directly read data from files stored in OneLake or external Azure storage accounts without prior ingestion into a table. It can be used for data exploration or data ingestion. 
-- 
-- Look at the code in the cell below and run it to create a sample data table and ingest the data from Azure storage into the dbo.CopyIntoExample table and view the data in the table.


-- CELL ********************

DROP TABLE IF EXISTS [dbo].[CopyIntoExample]
GO

CREATE TABLE [dbo].[CopyIntoExample]
    (
        [Date]                      [date]           NOT NULL,
        [DayNumber]                 [int]            NOT NULL,
        [Day]                       [varchar](10)    NOT NULL,
        [Month]                     [varchar](10)    NOT NULL,
        [ShortMonth]                [varchar](3)     NOT NULL,
        [CalendarMonthNumber]       [int]            NOT NULL,
        [CalendarMonthLabel]        [varchar](20)    NOT NULL,
        [CalendarYear]              [int]            NOT NULL,
        [CalendarYearLabel]         [varchar](10)    NOT NULL,
        [FiscalMonthNumber]         [int]            NOT NULL,
        [FiscalMonthLabel]          [varchar](20)    NOT NULL,
        [FiscalYear]                [int]            NOT NULL,
        [FiscalYearLabel]           [varchar](10)    NOT NULL,
        [ISOWeekNumber]             [int]            NOT NULL
    )
GO

/* Load the data from Azure storage using the COPY command */
COPY INTO dbo.CopyIntoExample FROM 'https://fabrictutorialdata.blob.core.windows.net/sampledata/WideWorldImportersDW/parquet/tables/DimDate.parquet' WITH (FILE_TYPE = 'PARQUET');

/* View the data in the table */
SELECT
    *
FROM dbo.CopyIntoExample

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- As metioned above, the OPENROWSET command is another powerful tool for exploring and loading data. Run the cell below to see how the OPENROWSET command can be used to read data directly from Azure storage and can be used just like any other table in the warehouse by joining, aliasing columns, and using other T-SQL commands. 

-- CELL ********************

/* View the data from Azure storage using the OPENROWSET command */

/* Using a line like INSERT INTO dbo.OPENROWSETExampleQuery here would allow you to transform and ingest data using OPENROWSET */
SELECT
    CASE WHEN cie.[Date] IS NOT NULL THEN 1 ELSE 0 END AS record_in_example_table,
    CONVERT(VARCHAR, d.[Date], 112) AS date_sk,
    d.[Date] AS [date],
    d.DayNumber AS day_number,
    d.[Day] AS [day],
    d.[Month] AS [month],
    d.ShortMonth AS short_month,
    d.CalendarMonthNumber AS calendar_month_number,
    d.CalendarMonthLabel AS calendar_month_label,
    d.CalendarYear AS calendar_year,
    d.CalendarYearLabel AS calendar_year_label,
    d.FiscalMonthNumber AS fiscal_month_number,
    d.FiscalMonthLabel AS fiscal_month_label,
    d.FiscalYear AS fiscal_year,
    d.FiscalYearLabel AS fiscal_year_label,
    d.ISOWeekNumber AS iso_week_number
FROM OPENROWSET(BULK 'https://fabrictutorialdata.blob.core.windows.net/sampledata/WideWorldImportersDW/parquet/tables/DimDate.parquet') AS d
FULL OUTER JOIN dbo.CopyIntoExample AS cie
    ON d.[Date] = cie.[Date]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- There are many ways to write the logic found in this part of the lab. We will be using one of the more common methods which involves incrementally pulling data from the silver layer and handling the INSERT and UPDATE logic in a single T-SQL MERGE statement. While the tables are setup to show how they can handle type 2 attribute logic there is no special handling for those attributes as the data generator does not change any existing records. 
-- 
-- **Incremental loads**
-- 
-- There are two main ways to load data: full and incremental. A full involves bringing in the entire source dataset and comparing it to the full warehouse dataset on each ETL run. On smaller dimension tables this may not be a problem. However, as fact tables grow larger this means the ETL runtime will be ever increasing. A more efficient method is to identify changes on the source system using change data capture or a date field and only bring over the changes. By doing this there is less load on the source system because it is pulling less data and there is less load on the warehouse as it only needs to process new and updated records. For this lab, we will implement a common incremental load pattern. 
-- 
-- To facilitate incrementally pulling data from the silver layer we will be creating an etl_tracking table, also known as a watermark table, in the dbo schema. This table will track the cutoff date, or last load date, for each table in the warehouse. This allows us to write the transformation logic in such a way that it ignores any records generated in the silver layer prior to the last ETL runtime. 
-- 
-- Each time a warehouse table is loaded the transformation logic will look at the etl_tracking table, get the last_load_datetime and use that in a WHERE clause on the query that pulls from the silver layer. Upon completion, the stored procedure will update the last_load_datetime so the next run of the ETL will only pick up new records from the time of the prior run. 
-- 
-- To ensure all the procedures are looking at the same slice of time when loading the gold layer we will write the stored procedures in such a way that they accept an optional date parameter. Because the procedures will not all run exactly parallel due to slight variations in start time this prevents the tables being loaded from slightly different time slices which could affect the consistency of the data across dimensions and facts. An example of the mismatched time slices is shown below along with how the watermark table will solve this.
-- 
-- | Table | Previous ETL Start Time | ETL Start Time | ETL End Time | Time Slice Used |
-- | -- | -- | -- | -- | -- |
-- | dim.customer | 2026-01-05 10:05:27 | 2026-01-06 10:09:16 | 2026-01-06 10:14:23 | 2026-01-05 10:05:27 to 2026-01-06 10:09:16
-- | dim.item | 2026-01-05 10:04:58 | 2026-01-06 10:05:48 | 2026-01-06 10:09:18 | 2026-01-05 10:04:58 to 2026-01-06 10:05:48
-- | fact.order | 2026-01-05 10:25:38 | 2026-01-06 10:28:04 | 2026-01-06 10:36:01 | 2026-01-05 10:25:38 to 2026-01-06 10:28:04
-- 
-- Notice how each table is using a different time slice when running. If you look closely you can see there is the possibility that an order record is generated at 10:15 on January 6 and gets loaded into the fact order table when the ETL starts running at 10:28. If that record is for a new customer which was also generated at 10:15 the data warehouse would include the order record but there would be no associated customer record because that ETL process has already completed. It would not be until the warehouse is loaded again that the new customer record will be included in the customer dimension and you'll need to fix the key value on the fact table. This leads to a large amount of ETL processing and potential confusion for end users. 
-- 
-- **One potential solution**
-- 
-- If we look at that same scenario but now use a common ETL batch start time that we pass into each procedure and we track the batch start time in our etl_tracking table, you can see that each time slice being considered is the same regardless of the variance in start/end times of each table's load.
-- 
-- 1. Batch start time is captured by an orchestration process, in this example ***2026-01-05 10:01:10***.
-- 1. Batch start time is passed into the stored procedure to load the warehouse table.
-- 1. Stored procedure looks up the last_load_datetime for the table from the etl_tracking table.
-- 1. Stored procedure parameterizes the WHERE statement to pull data from the last_load_datetime to the new batch start time passed into the procedure. 
-- 1. ETL logic is run to transform and incrementally load the table. 
-- 1. Stored procedure updates the last_load_datetime column in the etl_tracking table for the table being loaded using the batch start time that was passed into the procedure.
-- 
-- | Table | Previous ETL Start Time | ETL Start Time | ETL End Time | Time Slice Used |
-- | -- | -- | -- | -- | -- |
-- | dim.customer | 2026-01-05 10:01:18 | 2026-01-06 10:09:16 | 2026-01-06 10:14:23 | 2026-01-05 10:01:18 to 2026-01-05 10:01:10
-- | dim.item | 2026-01-05 10:01:18 | 2026-01-06 10:05:48 | 2026-01-06 10:09:18 | 2026-01-05 10:01:18 to 2026-01-05 10:01:10
-- | fact.order | 2026-01-05 10:01:18 | 2026-01-06 10:28:04 | 2026-01-06 10:36:01 | 2026-01-05 10:01:18 to 2026-01-05 10:01:10
-- 
-- This process ensures that each table load is looking at the same time slice. While there are errors that occur in ETL processes and this is not a foolproof method to ensure there is never an orphaned record, it is a great starting point and often good enough for many solutions. 
-- 
-- Now that we have an understanding of the method by which we will track incremental loads and the reason behind this method, run the following cell to create the etl_tracking table and populate it for the initial load by providing a last_load_datetime of 1900-01-01 to ensure all records in the silver tables are captured. 


-- CELL ********************

/* Drop the table if it exists, this is useful in case the script is run multiple times. */
DROP TABLE IF EXISTS [dbo].[etl_tracking]
GO

/* Create and populate the tracking table. */
CREATE TABLE [dbo].[etl_tracking]
    (
        [etl_tracking_id]       [int]           NOT NULL,
        [table_name]            [varchar](20)   NOT NULL,
        [last_load_datetime]    [datetime2](6)  NOT NULL        
    )

INSERT INTO [dbo].[etl_tracking]
VALUES
    (1, 'dim.address',         '1900-01-01 00:00:00'),
    (2, 'dim.customer',        '1900-01-01 00:00:00'),
    (3, 'dim.facility',        '1900-01-01 00:00:00'),
    (4, 'dim.item',            '1900-01-01 00:00:00'),
    (5, 'dim.order_source',    '1900-01-01 00:00:00'),
    (6, 'fact.order',          '1900-01-01 00:00:00'),
    (7, 'fact.shipment',       '1900-01-01 00:00:00')
GO

/* View the records in the table to ensure everything is setup for the first run. */
SELECT
    *
FROM dbo.etl_tracking
ORDER BY 
    table_name

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- Now that the incremental loading infrastructure is in place it's time to get started on data transformation. We will use stored procedures to encapsulate the transformation logic so it can be called interactively or through a pipeline. 
-- 
-- **Stored procedures** 
-- 
-- A stored procedure is a named, reusable block of SQL code saved inside a database. Instead of sending multiple SQL statements from an application, a stored procedure encapsulates that logic so you call it with a single execution. In a warehouse, this code will clean, transform, and perform INSERT and UPDATE logic. Each procedure accepts a date parameter which will be used as a cutoff date in the query against the silver layer tables. Effectively the logic is written so only records added to the silver layer after the start of the prior ETL run and before the date passed to the parameter are considered. At the end of the procedure the etl_tracking is updated using the date parameter value to reflect the new last_load_date. 
-- 
-- **MERGE** 
-- 
-- A T‑SQL MERGE statement is a single Data Manipulation Language (DML) command that can INSERT, UPDATE, and DELETE rows in a target table based on comparisons with a source table. Its purpose is to synchronize two datasets in one atomic, set‑based operation. An alternative to MERGE would be separate INSERT and UPDATE statements. 
-- 
-- **Cross database queries** 
-- 
-- Often in a data warehouse scenario you will create a stage schema where you can temporarily land new data before it is processed into the dimensional model. Becuase all the data for this lab is being fed through the medallion layers the data already exists in the silver layer. Since each Fabric lakehouse also has a SQL analytics endpoint we can query the data in place rather than needing to create an extra staging copy along the way. This is accomplished by using a 3-part name in the query in the form of *LakehouseOrWarehouseName.schema.table*.  
-- 
-- Explore the code below to see how various data engineering activities are being accomplished through T-SQL including:
-- 
-- - Renaming columns
-- - Joining and combining tables (denormalization)
-- - Calculating column values in the fact table
-- - Assigning unknown member values
-- - Handling type 1 and type 2 column changes
-- - Looking up surrogate key values in the fact table


-- CELL ********************

/***********     dim.address     ***********/
DROP PROCEDURE IF EXISTS dbo.load_dim_address
GO

CREATE PROCEDURE dbo.load_dim_address (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.address')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.address WHERE address_line_1 = 'Unknown' AND address_line_2 = 'Unknown' AND state_abbreviation = 'Unknown' AND zip_code = 'Unknown' AND country = 'Unknown')
    INSERT INTO dim.address VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0.0, 0.0)

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.address AS t
    USING 
        (
            SELECT
                destination_address AS address_line_1, 
                '' AS address_line_2,
                destination_city AS city,
                destination_state AS state_abbreviation,
                destination_zip_code AS zip_code,
                destination_country AS country,
                destination_latitude AS latitude,
                destination_longitude AS longitude
            FROM [SalesAndLogisticsLH].silver.shipment
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime

            UNION

            SELECT
                origin_address AS address_line_1, 
                '' AS address_line_2,
                origin_city AS city,
                origin_state AS state_abbreviation,
                origin_zip_code AS zip_code,
                origin_country AS country,
                origin_latitude AS latitude,
                origin_longitude AS longitude
            FROM [SalesAndLogisticsLH].silver.shipment
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.address_line_1 = s.address_line_1
        AND t.city = s.city
        AND t.state_abbreviation = s.state_abbreviation
        AND t.zip_code = s.zip_code
    WHEN MATCHED THEN
        UPDATE
        SET
            address_line_1 = s.address_line_1,
            address_line_2 = s.address_line_2,
            city = s.city,
            state_abbreviation = s.state_abbreviation,
            zip_code = s.zip_code,
            country = s.country,
            latitude = s.latitude,
            longitude = s.longitude
    WHEN NOT MATCHED THEN 
        INSERT (address_line_1, address_line_2, city, state_abbreviation, zip_code, country, latitude, longitude)
        VALUES(s.address_line_1, s.address_line_2, s.city, s.state_abbreviation, s.zip_code, s.country, s.latitude, s.longitude);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.address'
END
GO

/***********     dim.customer     ***********/
DROP PROCEDURE IF EXISTS dbo.load_dim_customer
GO

CREATE PROCEDURE dbo.load_dim_customer (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.customer')

    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.customer WHERE customer_ak = 'Unknown')
    INSERT INTO dim.customer VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, 0, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.customer AS t
    USING 
        (
            SELECT
                [customer_id] AS customer_ak,
                [customer_name],
                [description] AS customer_description,
                [primary_contact_first_name],
                [primary_contact_last_name],
                [primary_contact_email],
                [primary_contact_phone],
                [delivery_city],
                [delivery_country],
                [delivery_latitude],
                [delivery_longitude],
                [delivery_state],
                [delivery_zip_code],
                [billing_city],
                [billing_country],
                [billing_state],
                '1900-01-31' AS start_date,
                '2099-12-31' AS end_date
            FROM [SalesAndLogisticsLH].silver.customer
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.customer_ak = s.customer_ak
    WHEN MATCHED THEN
        UPDATE
        SET
            customer_ak                 = s.customer_ak,
            customer_name               = s.customer_name,
            customer_description        = s.customer_description,
            primary_contact_first_name  = s.primary_contact_first_name,
            primary_contact_last_name   = s.primary_contact_last_name,
            primary_contact_email       = s.primary_contact_email,
            primary_contact_phone       = s.primary_contact_phone,
            delivery_city               = s.delivery_city,
            delivery_country            = s.delivery_country,
            delivery_latitude           = s.delivery_latitude,
            delivery_longitude          = s.delivery_longitude,
            delivery_state              = s.delivery_state,
            delivery_zip_code           = s.delivery_zip_code,
            billing_city                = s.billing_city,
            billing_country             = s.billing_country,
            billing_state               = s.billing_state,
            start_date                  = s.start_date,
            end_date                    = s.end_date
    WHEN NOT MATCHED THEN 
        INSERT (customer_ak, customer_name, customer_description, primary_contact_first_name, primary_contact_last_name, primary_contact_email, primary_contact_phone, delivery_city, delivery_country, delivery_latitude, delivery_longitude, delivery_state, delivery_zip_code, billing_city, billing_country, billing_state, start_date, end_date)
        VALUES (s.customer_ak, s.customer_name, s.customer_description, s.primary_contact_first_name, s.primary_contact_last_name, s.primary_contact_email, s.primary_contact_phone, s.delivery_city, s.delivery_country, s.delivery_latitude, s.delivery_longitude, s.delivery_state, s.delivery_zip_code, s.billing_city, s.billing_country, s.billing_state, s.start_date, s.end_date);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.customer'
END
GO

/***********     dim.facility     ***********/
DROP PROCEDURE IF EXISTS dbo.load_dim_facility
GO

CREATE PROCEDURE dbo.load_dim_facility (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
     /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.facility')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.facility WHERE facility_ak = 'Unknown')
    INSERT INTO dim.facility VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0.0, 0.0, '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.facility AS t
    USING 
        (
            SELECT
                facility_id AS facility_ak,
                facility_name,
                facility_type,
                address,
                city,
                [state] AS state_abbreviation,
                zip_code,
                country,
                latitude,
                longitude,
                '1900-01-01' AS start_date,
                '12-31-2099' AS end_date
            FROM [SalesAndLogisticsLH].silver.facility
            WHERE 
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.facility_ak = s.facility_ak
    WHEN MATCHED THEN
        UPDATE
        SET
            facility_ak         = s.facility_ak,
            facility_name       = s.facility_name,
            facility_type       = s.facility_type,
            address             = s.address,
            city                = s.city,
            state_abbreviation  = s.state_abbreviation,
            zip_code            = s.zip_code
    WHEN NOT MATCHED THEN 
        INSERT (facility_ak, facility_name, facility_type, address, city, state_abbreviation, zip_code, country, latitude, longitude, start_date, end_date)
        VALUES(s.facility_ak, s.facility_name, s.facility_type, s.address, s.city, s.state_abbreviation, s.zip_code, s.country, s.latitude, s.longitude, s.start_date, s.end_date);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.facility'
END
GO

/***********     dim.item     ***********/
DROP PROCEDURE IF EXISTS dbo.load_dim_item
GO

CREATE PROCEDURE dbo.load_dim_item (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.item')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.item WHERE item_ak = 'Unknown')
    INSERT INTO dim.item VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, 'Unknown', 0, 0, 0, 0, 0, 0, 0, '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.item AS t
    USING 
        (
            SELECT
                item_id AS item_ak,
                sku,
                description AS item_description,
                brand,
                category,
                subcategory,
                material,
                nominal_size,
                end_connection,
                pressure_class,
                weight,
                cost,
                list_price,
                is_sdofcertified,
                structural_index,
                span_rating,
                '1900-01-01' AS start_date,
                '12-31-2099' AS end_date
            FROM [SalesAndLogisticsLH].silver.item
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.item_ak = s.item_ak
    WHEN MATCHED THEN
        UPDATE
        SET
            item_ak             = s.item_ak,
            sku                 = s.sku,
            item_description    = s.item_description,
            brand               = s.brand,
            category            = s.category,
            subcategory         = s.subcategory,
            material            = s.material
    WHEN NOT MATCHED THEN 
        INSERT (item_ak, sku, item_description, brand, category, subcategory, material, nominal_size, end_connection, pressure_class, weight, cost, list_price, is_sdofcertified, structural_index, span_rating, start_date, end_date)
        VALUES(s.item_ak, s.sku, s.item_description, s.brand, s.category, s.subcategory, s.material, s.nominal_size, s.end_connection, s.pressure_class, s.weight, s.cost, s.list_price, s.is_sdofcertified, s.structural_index, s.span_rating, s.start_date, s.end_date);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.item'
END
GO

/***********     dim.order_source     ***********/
DROP PROCEDURE IF EXISTS dbo.load_dim_order_source
GO

CREATE PROCEDURE dbo.load_dim_order_source (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.order_source')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.order_source WHERE order_source_ak = 'Unknown')
    INSERT INTO dim.order_source VALUES ('Unknown', 'Unknown', '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    INSERT INTO dim.order_source
    SELECT DISTINCT
        source AS order_source_ak,
        source AS order_source_name,
        '1900-01-01' AS start_date,
        '12-31-2099' AS end_date
    FROM [SalesAndLogisticsLH].silver.[order] AS o
    LEFT JOIN dim.order_source AS os
        ON o.source = os.order_source_ak
    WHERE
        os.order_source_ak IS NULL
        AND _processing_timestamp > @last_load_datetime
        AND _processing_timestamp <= @new_load_datetime

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.order_source'
END
GO

/***********     fact.order     ***********/
DROP PROCEDURE IF EXISTS dbo.load_fact_order
GO

CREATE PROCEDURE dbo.load_fact_order (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'fact.order')
    
    /* Insert new records from the lakehouse bronze tables */
    INSERT INTO fact.[order]
    SELECT
        o.order_number,
        o.line_number AS order_line_number,
        ISNULL(d.date_sk, 19000101) AS order_date_sk,
        ISNULL(os.order_source_sk, -1) AS order_source_sk,
        ISNULL(c.customer_sk, -1) AS customer_sk,
        ISNULL(i.item_sk, -1) AS item_sk,
        o.quantity,
        o.unit_price,
        o.extended_price,
        o.net_weight,
        o.warranty_included
    FROM [SalesAndLogisticsLH].silver.[order] AS o
    LEFT JOIN dim.order_source AS os
        ON o.source = os.order_source_ak
    LEFT JOIN dim.customer AS c
        ON o.customer_id = c.customer_ak
    LEFT JOIN dim.item AS i
        ON o.item_id = i.item_ak
    LEFT JOIN dim.[date] AS d
        ON CONVERT(DATE, o.order_date) = d.[date]
    WHERE
        NOT EXISTS
            (
                SELECT
                    1
                FROM fact.[order] AS fo
                WHERE
                    o.order_number = fo.order_number
                    AND o.line_number = fo.order_line_number
            )
        AND _processing_timestamp > @last_load_datetime
        AND _processing_timestamp <= @new_load_datetime
    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'fact.order'
END
GO

/***********     fact.shipment     ***********/
DROP PROCEDURE IF EXISTS dbo.load_fact_shipment
GO

CREATE PROCEDURE dbo.load_fact_shipment (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'fact.shipment')
    
    /* Insert new records from the lakehouse bronze tables */
    INSERT INTO fact.shipment
    SELECT DISTINCT
        s.tracking_number
        ,o.order_number
        ,ISNULL(sd.date_sk, 19000101) AS ship_date_sk
        ,ISNULL(cdd.date_sk, 19000101) AS committed_delivery_date_sk
        ,ISNULL(dd.date_sk, 19000101) AS delivery_date_sk
        ,ISNULL(c.[customer_sk], -1) AS customer_sk
        ,ISNULL(oa.address_sk, -1) AS origin_address_sk
        ,ISNULL(da.address_sk, -1) AS destination_address_sk
        ,s.service_level
        ,GREATEST(DATEDIFF(DAY, CONVERT(DATE, s.committed_delivery_date), CONVERT(DATE, delivery.delivery_date)), 0) AS delivery_days_late
        ,s.[late_delivery_penalty_per_day]
        ,GREATEST(DATEDIFF(DAY, CONVERT(DATE, s.committed_delivery_date), CONVERT(DATE, delivery.delivery_date)), 0) * s.late_delivery_penalty_per_day AS late_delivery_penalty
        ,s.distance AS shipment_distance
        ,s.declared_value
        ,s.height
        ,s.width
        ,s.length
        ,s.volume
        ,s.weight
        ,s.is_fragile
        ,s.is_hazardous
        ,s.requires_refrigeration
    FROM [SalesAndLogisticsLH].silver.shipment AS s
    LEFT JOIN [SalesAndLogisticsLH].silver.[order] AS o
        ON s.order_id = o.order_id
    LEFT JOIN dim.customer AS c
        ON s.customer_id = c.customer_ak
    LEFT JOIN dim.address AS da
        ON s.destination_address = da.address_line_1
        AND s.destination_city = da.city
        AND s.destination_state = da.state_abbreviation
        AND s.destination_zip_code = da.zip_code
    LEFT JOIN dim.address AS oa
        ON  s.origin_address = oa.address_line_1
        AND s.origin_city = oa.city
        AND s.origin_state = oa.state_abbreviation
        AND s.origin_zip_code = oa.zip_code
    LEFT JOIN dim.date AS sd
        ON CONVERT(DATE, s.ship_date) = sd.date
    LEFT JOIN dim.date AS cdd
        ON CONVERT(DATE, s.committed_delivery_date) = cdd.date
    LEFT JOIN (
        SELECT 
            shipment_id,
            CONVERT(DATE, event_timestamp) AS delivery_date
        FROM [SalesAndLogisticsLH].silver.shipment_scan_event
        WHERE event_type = 'Delivered'
    ) AS delivery
        ON s.shipment_id = delivery.shipment_id
    LEFT JOIN dim.date AS dd
        ON delivery.delivery_date = dd.date
    WHERE
        NOT EXISTS
            (
                SELECT
                    1
                FROM fact.shipment AS fs
                WHERE
                    s.tracking_number = fs.tracking_number
            )
        AND s._processing_timestamp > @last_load_datetime
        AND s._processing_timestamp <= @new_load_datetime

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'fact.shipment'
END
GO

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- Now that the stored procedures are created, run the next cell to execute the stored procedures. The script will collect a pre-load record count on the tables, run the load procedures, then collect a post-load record count. Notice the record counts on the tables before and after the procedure runs on the output. 
-- 
-- Optionally, wait a few minutes and run the cell again to see more data flowing in from the silver lakehouse.

-- CELL ********************

DECLARE @dim_address        BIGINT 
DECLARE @dim_customer       BIGINT
DECLARE @dim_date           BIGINT 
DECLARE @dim_facility       BIGINT
DECLARE @dim_item           BIGINT
DECLARE @dim_order_source   BIGINT
DECLARE @fact_order         BIGINT
DECLARE @fact_shipment      BIGINT

/* Get the record count before running the procedures */
SELECT
    @dim_address        = (SELECT COUNT_BIG(*) FROM dim.address),
    @dim_customer       = (SELECT COUNT_BIG(*) FROM dim.customer),
    @dim_date           = (SELECT COUNT_BIG(*) FROM dim.[date]),
    @dim_facility       = (SELECT COUNT_BIG(*) FROM dim.facility),
    @dim_item           = (SELECT COUNT_BIG(*) FROM dim.item),
    @dim_order_source   = (SELECT COUNT_BIG(*) FROM dim.order_source),
    @fact_order         = (SELECT COUNT_BIG(*) FROM fact.[order]),
    @fact_shipment      = (SELECT COUNT_BIG(*) FROM fact.shipment)

/* Run the stored procedures, passing in the same load time for each procedure */
DECLARE @data_warehouse_load_time DATETIME2(6) = GETDATE()

EXEC load_dim_address       @data_warehouse_load_time
EXEC load_dim_customer      @data_warehouse_load_time
EXEC load_dim_facility      @data_warehouse_load_time
EXEC load_dim_item          @data_warehouse_load_time
EXEC load_dim_order_source  @data_warehouse_load_time
EXEC load_fact_order        @data_warehouse_load_time
EXEC load_fact_shipment     @data_warehouse_load_time

/* Get the record count after running the procedures and compare them to the before numbers */
SELECT @dim_address        AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @dim_address        AS record_count_change, 'dim.address'       AS table_name FROM dim.address        UNION ALL
SELECT @dim_customer       AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @dim_customer       AS record_count_change, 'dim.customer'      AS table_name FROM dim.customer       UNION ALL
SELECT @dim_date           AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @dim_date           AS record_count_change, 'dim.date'          AS table_name FROM dim.[date]         UNION ALL
SELECT @dim_facility       AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @dim_facility       AS record_count_change, 'dim.facility'      AS table_name FROM dim.facility       UNION ALL
SELECT @dim_item           AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @dim_item           AS record_count_change, 'dim.item'          AS table_name FROM dim.item           UNION ALL
SELECT @dim_order_source   AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @dim_order_source   AS record_count_change, 'dim.order_source'  AS table_name FROM dim.order_source   UNION ALL
SELECT @fact_order         AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @fact_order         AS record_count_change, 'fact.order'        AS table_name FROM fact.[order]       UNION ALL
SELECT @fact_shipment      AS record_count_before, COUNT_BIG(*) AS record_count_after, COUNT_BIG(*) - @fact_shipment      AS record_count_change, 'fact.shipment'     AS table_name FROM fact.shipment

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## ⚙️ Part 4: Beyond the Basics
-- 
-- Let's peel back the covers and take a look under the hood to see what happens when queries are run on the Fabric data warehouse. How do you monitor query activity and how do we achieve performance by default with the SQL engine. Items in this section are applicable to both the warehouse and the SQL analytics endpoint. 
-- 
-- **Caching**
-- 
-- Caching is a technique that improves the performance of data processing applications by reducing the IO operations. Caching stores frequently accessed data and metadata in a faster storage layer, such as local memory or local SSD disk, so that subsequent requests can be served more quickly, directly from the cache. If a particular set of data has been previously accessed by a query, any subsequent queries retrieve that data directly from the in-memory cache. This approach significantly diminishes IO latency, as local memory operations are notably faster compared to fetching data from remote storage.
-- 
-- In-memory and disk caching in Fabric Data Warehouse is fully transparent to the user. Irrespective of the origin, whether it be a warehouse table, a OneLake shortcut, or even OneLake shortcut that references to non-Azure services, the query caches all the data it accesses.
-- 
-- As the query accesses and retrieves data from storage, it performs a transformation process that transcodes the data from its original file-based format into highly optimized structures in in-memory cache.
-- 
-- ![Populating the in memory cache](https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/warehouse/populating-in-memory-cache.png?raw=true)
-- 
-- Certain datasets are too large to be accommodated within an in-memory cache. To sustain rapid query performance for these datasets, Warehouse utilizes disk space as a complementary extension to the in-memory cache. Any information that is loaded into the in-memory cache is also serialized to the SSD cache.
-- 
-- ![Populating the in memory and SSD caches](https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/warehouse/populating-in-memory-and-ssd-cache.png?raw=true)
-- 
-- Given that the in-memory cache has a smaller capacity compared to the SSD cache, data that is removed from the in-memory cache remains within the SSD cache for an extended period. When subsequent query requests this data, it's retrieved from the SSD cache into the in-memory cache significantly quicker than if fetched from remote storage, ultimately providing you with more consistent query performance.
-- 
-- ![Populating the in memory cache from SSD cache](https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/warehouse/populating-in-memory-cache-from-ssd-cache.png?raw=true)
-- 
-- **Statistics**
-- 
-- The Warehouse in Microsoft Fabric uses a query engine to create an execution plan for a given SQL query. When you submit a query, the query optimizer tries to enumerate all possible plans and choose the most efficient candidate. As is traditionally done, statistics can be created and updated manually. Alternatively, the engine will create statistics on an as-needed basis when queries are run and then subsequently refresh those statistics when the data in the table is changed. 
-- 
-- Let's explore how automatic statistics creation works by running the query below to see the existing statistics on the dim.customer table. Notice there will be already be several stats created on the table. These will align with the columns that we have run SELECT statements on during the ETL process (customer_sk and customer_ak).


-- CELL ********************

SELECT
    SCHEMA_NAME(o.schema_id) AS [schema_name],
    object_name(s.object_id) AS [table_name],
    c.name AS [column_name],
    s.name AS [stats_name],
    s.stats_id,
    STATS_DATE(s.object_id, s.stats_id) AS [stats_update_date], 
    s.auto_created,
    s.user_created,
    s.stats_generation_method_desc 
FROM sys.stats AS s 
INNER JOIN sys.objects AS o 
    ON o.object_id = s.object_id 
LEFT JOIN sys.stats_columns AS sc 
    ON s.object_id = sc.object_id 
    AND s.stats_id = sc.stats_id 
LEFT JOIN sys.columns AS c 
    ON sc.object_id = c.object_id 
    AND c.column_id = sc.column_id
WHERE
    o.type = 'U' /* Only check for stats on user-tables */
    AND
        (
            s.auto_created = 1
            OR s.user_created = 1
        )
    AND SCHEMA_NAME(o.schema_id) = 'dim'
    AND o.name = 'customer'
ORDER BY
    schema_name,
    table_name,
    column_name

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- Now, run a T-SQL query that aggregates data using another column in the table. This will trigger an automatic creation of statistics on those columns.

-- CELL ********************

SELECT
    COUNT(*) AS customer_count,
    delivery_state,
    billing_state
FROM dim.customer
GROUP BY
    delivery_state,
    billing_state
ORDER BY
    customer_count DESC

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- Return to the query to check statistics and notice there are now additional statistics that have been create on the delivery_state and billing_state columns. As the data in the table is updated those statistics will be automatically updated. No longer will your DBAs need to build a statistics creation and maintenance processes, the data warehouse has automated this task that is vital to ensuring optimal performance!

-- CELL ********************

SELECT
    SCHEMA_NAME(o.schema_id) AS [schema_name],
    object_name(s.object_id) AS [table_name],
    c.name AS [column_name],
    s.name AS [stats_name],
    s.stats_id,
    STATS_DATE(s.object_id, s.stats_id) AS [stats_update_date], 
    s.auto_created,
    s.user_created,
    s.stats_generation_method_desc 
FROM sys.stats AS s 
INNER JOIN sys.objects AS o 
    ON o.object_id = s.object_id 
LEFT JOIN sys.stats_columns AS sc 
    ON s.object_id = sc.object_id 
    AND s.stats_id = sc.stats_id 
LEFT JOIN sys.columns AS c 
    ON sc.object_id = c.object_id 
    AND c.column_id = sc.column_id
WHERE
    o.type = 'U' /* Only check for stats on user-tables */
    AND
        (
            s.auto_created = 1
            OR s.user_created = 1
        )
    AND SCHEMA_NAME(o.schema_id) = 'dim'
    AND o.name = 'customer'
ORDER BY
    schema_name,
    table_name,
    column_name

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- **Time travel**
-- 
-- Time travel unlocks the ability to query the prior versions of data without the need to generate multiple data copies, saving on storage costs. This article describes how to query warehouse tables using time travel at the statement level, using the T-SQL OPTION clause and the FOR TIMESTAMP AS OF syntax.
-- 
-- **Table Clones**
-- 
-- In Fabric Data Warehouse you can create a new table as a zero-copy clone of another table. Only the metadata of the table is copied. The underlying data of the table, stored as parquet files, is not copied. Clones can be created as of a specific point in time allowing for interesting scenarios like data recovery. 
-- 
-- The example below combines time travel for troubleshooting with table clones for data recovery. The code will generate sample records, wait a few seconds, "accidentally" delete some data, use time travel to view the changes in the records and locate a version of the record which can be recovered, and then use table clones to recover the deleted record. As you step through the code, change the result set in the top right corner of the results grid.
-- 
-- ![Time travel and clone table demo](https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/warehouse/time-travel-and-clone.gif?raw=true)


-- CELL ********************

/* Drop the example simulation tables in case they exist */
DROP TABLE IF EXISTS dbo.time_travel_simulation
DROP TABLE IF EXISTS dbo.time_travel_simulation_recovery

/* Create a small table to store a few example values */
CREATE TABLE dbo.time_travel_simulation
    (
        id              INT,
        name            VARCHAR(50),
        miles_driven    INT
    )

/* Add three records to the table */
INSERT INTO dbo.time_travel_simulation
VALUES 
    (1, 'Tim', 150),
    (2, 'Susan', 85),
    (3, 'Fred', 70)

/* Result 1: Verify all three records were added to the table */
SELECT * FROM dbo.time_travel_simulation

/* Wait 5 seconds before simulating a record being deleted by accident */
WAITFOR DELAY '00:00:05'

DECLARE @BeforeTheAccident VARCHAR(23) = (CONVERT(VARCHAR, GETDATE(), 25))

/* Oh no! Someone wrote a bad query and deleted Tim's record from the table! */
DELETE FROM dbo.time_travel_simulation WHERE id = 1

/* Result 2: Look at the records in the table, Tim's record is gone! */
SELECT * FROM dbo.time_travel_simulation

/* Result 3: It's ok, the record's history still exists. We can validate that using Time Travel! */
EXEC ('SELECT * FROM dbo.time_travel_simulation OPTION (FOR TIMESTAMP AS OF ''' + @BeforeTheAccident + ''')');

/* We can even recover the data by creating a clone of the table when Tim's record still existed and moving it back into the dbo.time_travel_simulation table */
EXEC ('CREATE TABLE dbo.time_travel_simulation_recovery AS CLONE OF dbo.time_travel_simulation AT ''' + @BeforeTheAccident + '''')

/* With the table cloned, we can add Tim's record back to the dbo.time_travel_simulation table and verify the recovery is complete */
INSERT INTO dbo.time_travel_simulation
SELECT * FROM dbo.time_travel_simulation_recovery WHERE id = 1

/* Result 4: Validate Tim's record has been recovered */
SELECT * FROM dbo.time_travel_simulation

/* Cleanup the sample tables */
DROP TABLE IF EXISTS dbo.time_travel_simulation
DROP TABLE IF EXISTS dbo.time_travel_simulation_recovery

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 🏭 Part 5: Operationalize Warehouse Loading
-- 
-- *This portion of the lab is informational only.*
-- 
-- All the objects needed to load the data warehouse (gold layer) are now in place! There is only one step left to operationalize the execution of the stored procedures. Right now, they need to be manually executed. Ideally, you will create a Data Factory pipeline to execute the procedures in the correct order and schedule it. 
-- 
-- One way to build the pipeline is as follows:
-- 
-- 1. Return to the workspace and create a new item. 
-- 1. From the **Get data** or **Prepare data** sections of the New item list, select **Pipeline**. Give the pipeline a name when prompted such as *Load Gold Layer*. 
-- 1. Add a series of **Stored procedure** activities which can be found in the **Transform** section of the activity list. Add 7 to the canvas; one for each stored procedure that needs to be run. 
-- 1. Configure each activity to run one of the stored procedures. 
-- 1. Setup the constraints so the dimensions all load in parallel, followed by the fact.order table, followed by the fact.shipment table. This takes into account all the loading dependencies. 
-- 1. Finally, save and schedule the pipeline to run at the frequency of your choosing. 
-- 
-- ![Completed Data Factory orchestration pipeline](https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/warehouse/pipeline.png?raw=true)


-- MARKDOWN ********************

-- ---
-- 
-- ## 🎓 Key Takeaways & Next Steps
-- 
-- ### 🏆 What You've Accomplished
-- 
-- Congratulations! You've explored the basics of data warehousing within a medallion architecture in Microsoft Fabric. Here's what you've learned:
-- 
-- #### 1. Data Warehouse Fundamentals
-- - **Star schema**: The data warehouse engine is optimized for large batch reads/writes in a start schema
-- - **Facts**: The business events that occur (associated with action words: sales, shipments, calls, page views, etc.)
-- - **Dimensions**: Describe a fact and are ways to slice and dice a fact (by customer, by date, by item, etc.)
-- - **Slowly changing dimensions**: Used to track updates and history over time in a dimension, most commonly Type 0, 1, and 2
-- 
-- #### 2. Working with schemas and tables
-- - **Table organization**: Used to group tables together into logical groups like fact/dim, business unit, or source system
-- - **Securing your data**: Schemas are the first logical level to restrict access followed by tables then rows within a table
-- - **Data types**: The query optimizer takes data types into account when building a query plan, smallest data type needed should be chosen
-- 
-- #### 3. Loading and transforming data with T-SQL
-- - **Cross database queries**: Useful for loading data from a staging warehouse or a lakehouse, reference any database in the workspace with a 3-part name
-- - **Multiple ingestion options**: Use COPY INTO for maximum throughput and OPENROWSET for data exploration
-- - **Data Factory**: Use pipelines and COPY Job to load tables, but beware of the performance difference compared to COPY INTO, OPENROWSET, or cross database queries
-- - **Data transformation**: Wrap logic in stored procedures for easy reference in orchestration tools; be sure to handle unknown members and late arriving facts
-- 
-- #### 4. Beyond the basics
-- - **Caching**: The warehouse caches data in memory and in local SSDs on compute nodes
-- - **Statistics**: Auto, proactive and incremental statistics help create and maintain stats automatically for maximum query performance
-- - **Time travel and clones**: These features are excellent tools for historical analysis and data recovery
-- 
-- #### 5. Operationalizing warehouse loading
-- - **Data Factory**: Use stored procedure activities to execute code on the warehouse ensuring dependencies are accounted for when tables are loaded
-- - **Watermark table**: Use a watermark table or a control table to track the last load time for each table to build incremental loading logic
-- - **Scheduling a pipeline**: Pipelines can be scheduled to run at whatever frequency required by the business but be cautious about the impact of trickle loading data
-- 
-- ---
-- 
-- ### 🚀 What's Next?
-- 
-- Continue your journey through the McMillan Industrial Group data pipeline:
-- 
-- | Experience | What You'll Learn |
-- |----------|-------------------|
-- | **🤖 3_create_data_agent** Notebook | Chat with your data using natural language via a Data Agent |
-- 
-- ### 📚 Additional Resources
-- 
-- Expand your knowledge with these official docs:
-- 
-- - [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
-- - [Better together: the lakehouse and warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/get-started-lakehouse-sql-analytics-endpoint)
-- - [Dimensional modeling in Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/dimensional-modeling-overview)
-- - [Migrate to Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/migration-assistant)
-- 
-- ---
-- 
-- **🎉 Great work completing this notebook!** You have built and entire end-to-end data pipeline that generates data and moves it through all three layers in a medallion architecture. Move on to the next notebook when you're ready to build the data agent! 🚀

