CREATE TABLE [dbo].[Sales CustomerTransactions] (

	[CustomerTransactionID] int NULL, 
	[CustomerID] int NULL, 
	[TransactionTypeID] int NULL, 
	[InvoiceID] int NULL, 
	[PaymentMethodID] int NULL, 
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