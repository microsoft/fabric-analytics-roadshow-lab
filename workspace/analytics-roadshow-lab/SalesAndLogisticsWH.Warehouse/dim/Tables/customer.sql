CREATE TABLE [dim].[customer] (

	[customer_sk] bigint IDENTITY NOT NULL, 
	[customer_ak] varchar(50) NOT NULL, 
	[customer_name] varchar(50) NOT NULL, 
	[customer_description] varchar(100) NOT NULL, 
	[primary_contact_first_name] varchar(50) NOT NULL, 
	[primary_contact_last_name] varchar(50) NOT NULL, 
	[primary_contact_email] varchar(50) NOT NULL, 
	[primary_contact_phone] varchar(30) NOT NULL, 
	[delivery_city] varchar(50) NOT NULL, 
	[delivery_country] varchar(10) NOT NULL, 
	[delivery_latitude] float NOT NULL, 
	[delivery_longitude] float NOT NULL, 
	[delivery_state] varchar(10) NOT NULL, 
	[delivery_zip_code] varchar(10) NOT NULL, 
	[billing_city] varchar(50) NOT NULL, 
	[billing_country] varchar(10) NOT NULL, 
	[billing_state] varchar(10) NOT NULL, 
	[start_date] datetime2(6) NOT NULL, 
	[end_date] datetime2(6) NOT NULL
);