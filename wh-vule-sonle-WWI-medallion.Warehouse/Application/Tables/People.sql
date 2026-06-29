CREATE TABLE [Application].[People] (

	[PersonID] int NULL, 
	[FullName] varchar(8000) NULL, 
	[PreferredName] varchar(8000) NULL, 
	[SearchName] varchar(8000) NULL, 
	[IsPermittedToLogon] bit NULL, 
	[LogonName] varchar(8000) NULL, 
	[IsExternalLogonProvider] bit NULL, 
	[HashedPassword] varbinary(8000) NULL, 
	[IsSystemUser] bit NULL, 
	[IsEmployee] bit NULL, 
	[IsSalesperson] bit NULL, 
	[UserPreferences] varchar(8000) NULL, 
	[PhoneNumber] varchar(8000) NULL, 
	[FaxNumber] varchar(8000) NULL, 
	[EmailAddress] varchar(8000) NULL, 
	[Photo] varbinary(8000) NULL, 
	[CustomFields] varchar(8000) NULL, 
	[OtherLanguages] varchar(8000) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);