CREATE TABLE [dim].[order_source] (

	[order_source_sk] bigint IDENTITY NOT NULL, 
	[order_source_ak] varchar(10) NOT NULL, 
	[order_source_name] varchar(10) NOT NULL, 
	[start_date] datetime2(6) NOT NULL, 
	[end_date] datetime2(6) NOT NULL
);