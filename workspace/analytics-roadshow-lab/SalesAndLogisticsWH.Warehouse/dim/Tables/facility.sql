CREATE TABLE [dim].[facility] (

	[facility_sk] bigint IDENTITY NOT NULL, 
	[facility_ak] varchar(10) NOT NULL, 
	[facility_name] varchar(50) NOT NULL, 
	[facility_type] varchar(25) NOT NULL, 
	[address] varchar(50) NOT NULL, 
	[city] varchar(50) NOT NULL, 
	[state_abbreviation] varchar(10) NOT NULL, 
	[zip_code] varchar(10) NOT NULL, 
	[country] varchar(10) NOT NULL, 
	[latitude] float NOT NULL, 
	[longitude] float NOT NULL, 
	[start_date] datetime2(6) NOT NULL, 
	[end_date] datetime2(6) NOT NULL
);