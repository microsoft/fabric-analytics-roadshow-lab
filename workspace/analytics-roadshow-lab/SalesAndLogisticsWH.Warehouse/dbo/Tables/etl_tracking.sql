CREATE TABLE [dbo].[etl_tracking] (

	[etl_tracking_id] int NOT NULL, 
	[table_name] varchar(20) NOT NULL, 
	[last_load_datetime] datetime2(6) NOT NULL
);