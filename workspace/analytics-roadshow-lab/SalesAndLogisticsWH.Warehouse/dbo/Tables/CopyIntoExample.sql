CREATE TABLE [dbo].[CopyIntoExample] (

	[Date] date NOT NULL, 
	[DayNumber] int NOT NULL, 
	[Day] varchar(10) NOT NULL, 
	[Month] varchar(10) NOT NULL, 
	[ShortMonth] varchar(3) NOT NULL, 
	[CalendarMonthNumber] int NOT NULL, 
	[CalendarMonthLabel] varchar(20) NOT NULL, 
	[CalendarYear] int NOT NULL, 
	[CalendarYearLabel] varchar(10) NOT NULL, 
	[FiscalMonthNumber] int NOT NULL, 
	[FiscalMonthLabel] varchar(20) NOT NULL, 
	[FiscalYear] int NOT NULL, 
	[FiscalYearLabel] varchar(10) NOT NULL, 
	[ISOWeekNumber] int NOT NULL
);