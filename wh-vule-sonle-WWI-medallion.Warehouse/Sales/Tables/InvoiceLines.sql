CREATE TABLE [Sales].[InvoiceLines] (

	[InvoiceLineID] int NULL, 
	[InvoiceID] int NULL, 
	[StockItemID] int NULL, 
	[Description] varchar(8000) NULL, 
	[PackageTypeID] int NULL, 
	[Quantity] int NULL, 
	[UnitPrice] decimal(18,2) NULL, 
	[TaxRate] decimal(18,3) NULL, 
	[TaxAmount] decimal(18,2) NULL, 
	[LineProfit] decimal(18,2) NULL, 
	[ExtendedPrice] decimal(18,2) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);