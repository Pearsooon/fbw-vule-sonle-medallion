CREATE TABLE [dbo].[Application StateProvinces] (

	[StateProvinceID] int NULL, 
	[StateProvinceCode] varchar(8000) NULL, 
	[StateProvinceName] varchar(8000) NULL, 
	[CountryID] int NULL, 
	[SalesTerritory] varchar(8000) NULL, 
	[Border] varchar(8000) NULL, 
	[LatestRecordedPopulation] bigint NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);