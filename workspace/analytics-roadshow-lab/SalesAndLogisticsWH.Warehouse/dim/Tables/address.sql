CREATE TABLE [dim].[address] (

	[address_sk] bigint IDENTITY NOT NULL, 
	[address_line_1] varchar(50) NOT NULL, 
	[address_line_2] varchar(50) NOT NULL, 
	[city] varchar(50) NOT NULL, 
	[state_abbreviation] varchar(10) NOT NULL, 
	[zip_code] varchar(10) NOT NULL, 
	[country] varchar(10) NOT NULL, 
	[latitude] float NOT NULL, 
	[longitude] float NOT NULL
);