CREATE TABLE [dbo].[Purchasing PurchaseOrders] (

	[PurchaseOrderID] int NULL, 
	[SupplierID] int NULL, 
	[OrderDate] date NULL, 
	[DeliveryMethodID] int NULL, 
	[ContactPersonID] int NULL, 
	[ExpectedDeliveryDate] date NULL, 
	[SupplierReference] varchar(8000) NULL, 
	[IsOrderFinalized] bit NULL, 
	[Comments] varchar(8000) NULL, 
	[InternalComments] varchar(8000) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);