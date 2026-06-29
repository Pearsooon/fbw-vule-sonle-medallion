CREATE TABLE [dbo].[Purchasing SupplierTransactions] (

	[SupplierTransactionID] int NULL, 
	[SupplierID] int NULL, 
	[TransactionTypeID] int NULL, 
	[PurchaseOrderID] int NULL, 
	[PaymentMethodID] int NULL, 
	[SupplierInvoiceNumber] varchar(8000) NULL, 
	[TransactionDate] date NULL, 
	[AmountExcludingTax] decimal(38,6) NULL, 
	[TaxAmount] decimal(38,6) NULL, 
	[TransactionAmount] decimal(38,6) NULL, 
	[OutstandingBalance] decimal(38,6) NULL, 
	[FinalizationDate] date NULL, 
	[IsFinalized] bit NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);