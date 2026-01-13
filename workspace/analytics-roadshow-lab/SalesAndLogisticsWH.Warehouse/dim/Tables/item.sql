CREATE TABLE [dim].[item] (

	[item_sk] bigint IDENTITY NOT NULL, 
	[item_ak] varchar(50) NOT NULL, 
	[sku] varchar(25) NOT NULL, 
	[item_description] varchar(500) NOT NULL, 
	[brand] varchar(50) NOT NULL, 
	[category] varchar(50) NOT NULL, 
	[subcategory] varchar(50) NOT NULL, 
	[material] varchar(50) NOT NULL, 
	[nominal_size] float NOT NULL, 
	[end_connection] varchar(25) NOT NULL, 
	[pressure_class] bigint NOT NULL, 
	[weight] float NOT NULL, 
	[cost] float NOT NULL, 
	[list_price] float NOT NULL, 
	[is_sdofcertified] bit NOT NULL, 
	[structural_index] float NOT NULL, 
	[span_rating] float NOT NULL, 
	[start_date] datetime2(6) NOT NULL, 
	[end_date] datetime2(6) NOT NULL
);