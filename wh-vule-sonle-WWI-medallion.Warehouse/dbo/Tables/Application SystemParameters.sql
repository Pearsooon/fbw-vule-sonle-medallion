CREATE TABLE [dbo].[Application SystemParameters] (

	[SystemParameterID] int NULL, 
	[DeliveryAddressLine1] varchar(8000) NULL, 
	[DeliveryAddressLine2] varchar(8000) NULL, 
	[DeliveryCityID] int NULL, 
	[DeliveryPostalCode] varchar(8000) NULL, 
	[DeliveryLocation] varchar(8000) NULL, 
	[PostalAddressLine1] varchar(8000) NULL, 
	[PostalAddressLine2] varchar(8000) NULL, 
	[PostalCityID] int NULL, 
	[PostalPostalCode] varchar(8000) NULL, 
	[ApplicationSettings] varchar(8000) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);