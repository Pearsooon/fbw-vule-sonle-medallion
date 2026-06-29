CREATE TABLE [Purchasing].[PurchaseOrderLines] (

	[PurchaseOrderLineID] int NULL, 
	[PurchaseOrderID] int NULL, 
	[StockItemID] int NULL, 
	[OrderedOuters] int NULL, 
	[Description] varchar(8000) NULL, 
	[ReceivedOuters] int NULL, 
	[PackageTypeID] int NULL, 
	[ExpectedUnitPricePerOuter] decimal(18,2) NULL, 
	[LastReceiptDate] date NULL, 
	[IsOrderLineFinalized] bit NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);