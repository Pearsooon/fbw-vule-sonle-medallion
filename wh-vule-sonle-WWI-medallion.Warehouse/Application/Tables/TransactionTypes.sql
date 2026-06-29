CREATE TABLE [Application].[TransactionTypes] (

	[TransactionTypeID] int NULL, 
	[TransactionTypeName] varchar(8000) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);