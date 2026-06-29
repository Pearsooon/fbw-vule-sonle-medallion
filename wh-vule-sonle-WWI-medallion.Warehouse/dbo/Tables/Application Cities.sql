CREATE TABLE [dbo].[Application Cities] (

	[CityID] int NULL, 
	[CityName] varchar(8000) NULL, 
	[StateProvinceID] int NULL, 
	[Location] varchar(8000) NULL, 
	[LatestRecordedPopulation] bigint NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);