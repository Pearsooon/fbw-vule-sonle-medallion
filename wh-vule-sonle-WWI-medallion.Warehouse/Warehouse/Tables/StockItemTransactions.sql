CREATE TABLE [Warehouse].[StockItemTransactions] (

	[StockItemTransactionID] int NULL, 
	[StockItemID] int NULL, 
	[TransactionTypeID] int NULL, 
	[CustomerID] int NULL, 
	[InvoiceID] int NULL, 
	[SupplierID] int NULL, 
	[PurchaseOrderID] int NULL, 
	[TransactionOccurredWhen] datetime2(6) NULL, 
	[Quantity] decimal(18,3) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);