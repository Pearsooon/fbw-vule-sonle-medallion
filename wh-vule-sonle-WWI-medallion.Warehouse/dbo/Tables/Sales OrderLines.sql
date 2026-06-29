CREATE TABLE [dbo].[Sales OrderLines] (

	[OrderLineID] int NULL, 
	[OrderID] int NULL, 
	[StockItemID] int NULL, 
	[Description] varchar(8000) NULL, 
	[PackageTypeID] int NULL, 
	[Quantity] int NULL, 
	[UnitPrice] decimal(38,6) NULL, 
	[TaxRate] decimal(38,6) NULL, 
	[PickedQuantity] int NULL, 
	[PickingCompletedWhen] datetime2(6) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);