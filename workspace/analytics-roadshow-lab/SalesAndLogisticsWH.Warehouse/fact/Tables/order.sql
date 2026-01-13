CREATE TABLE [fact].[order] (

	[order_sk] bigint IDENTITY NOT NULL, 
	[order_number] varchar(25) NOT NULL, 
	[order_line_number] smallint NOT NULL, 
	[order_date_sk] int NOT NULL, 
	[order_source_sk] bigint NOT NULL, 
	[customer_sk] bigint NOT NULL, 
	[item_sk] bigint NOT NULL, 
	[quantity] bigint NULL, 
	[unit_price] float NULL, 
	[extended_price] float NULL, 
	[net_weight] float NULL, 
	[warranty_included] bit NULL
);