CREATE TABLE [dim].[date] (

	[date_sk] int NOT NULL, 
	[date] date NOT NULL, 
	[day_number] varchar(7) NOT NULL, 
	[day_of_week] varchar(9) NOT NULL, 
	[month_number] varchar(7) NOT NULL, 
	[month_name] varchar(9) NOT NULL, 
	[quarter] smallint NOT NULL, 
	[year] smallint NOT NULL
);