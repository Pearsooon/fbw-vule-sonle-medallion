CREATE TABLE [Sales].[OrderLines] (

	[OrderLineID] int NULL, 
	[OrderID] int NULL, 
	[StockItemID] int NULL, 
	[Description] varchar(8000) NULL, 
	[PackageTypeID] int NULL, 
	[Quantity] int NULL, 
	[UnitPrice] decimal(18,2) NULL, 
	[TaxRate] decimal(18,3) NULL, 
	[PickedQuantity] int NULL, 
	[PickingCompletedWhen] datetime2(6) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);