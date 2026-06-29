CREATE TABLE [dbo].[Sales InvoiceLines] (

	[InvoiceLineID] int NULL, 
	[InvoiceID] int NULL, 
	[StockItemID] int NULL, 
	[Description] varchar(8000) NULL, 
	[PackageTypeID] int NULL, 
	[Quantity] int NULL, 
	[UnitPrice] decimal(38,6) NULL, 
	[TaxRate] decimal(38,6) NULL, 
	[TaxAmount] decimal(38,6) NULL, 
	[LineProfit] decimal(38,6) NULL, 
	[ExtendedPrice] decimal(38,6) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);