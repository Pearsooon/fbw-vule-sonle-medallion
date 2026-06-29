CREATE TABLE [Warehouse].[StockItemHoldings] (

	[StockItemID] int NULL, 
	[QuantityOnHand] int NULL, 
	[BinLocation] varchar(8000) NULL, 
	[LastStocktakeQuantity] int NULL, 
	[LastCostPrice] decimal(18,2) NULL, 
	[ReorderLevel] int NULL, 
	[TargetStockLevel] int NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);