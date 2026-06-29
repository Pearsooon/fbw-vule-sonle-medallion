CREATE TABLE [Application].[DeliveryMethods_Archive] (

	[DeliveryMethodID] int NULL, 
	[DeliveryMethodName] varchar(8000) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);