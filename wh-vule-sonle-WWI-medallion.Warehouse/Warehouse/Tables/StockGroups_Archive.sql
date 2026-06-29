CREATE TABLE [Warehouse].[StockGroups_Archive] (

	[StockGroupID] int NULL, 
	[StockGroupName] varchar(8000) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);