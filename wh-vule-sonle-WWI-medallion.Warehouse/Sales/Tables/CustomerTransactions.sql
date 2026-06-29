CREATE TABLE [Sales].[CustomerTransactions] (

	[CustomerTransactionID] int NULL, 
	[CustomerID] int NULL, 
	[TransactionTypeID] int NULL, 
	[InvoiceID] int NULL, 
	[PaymentMethodID] int NULL, 
	[TransactionDate] date NULL, 
	[AmountExcludingTax] decimal(18,2) NULL, 
	[TaxAmount] decimal(18,2) NULL, 
	[TransactionAmount] decimal(18,2) NULL, 
	[OutstandingBalance] decimal(18,2) NULL, 
	[FinalizationDate] date NULL, 
	[IsFinalized] bit NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);