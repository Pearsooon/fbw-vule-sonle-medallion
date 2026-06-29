CREATE TABLE [dbo].[Application PaymentMethods] (

	[PaymentMethodID] int NULL, 
	[PaymentMethodName] varchar(8000) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);