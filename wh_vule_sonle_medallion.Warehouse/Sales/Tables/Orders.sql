CREATE TABLE [Sales].[Orders] (

	[OrderID] int NULL, 
	[CustomerID] int NULL, 
	[SalespersonPersonID] int NULL, 
	[PickedByPersonID] int NULL, 
	[ContactPersonID] int NULL, 
	[BackorderOrderID] int NULL, 
	[OrderDate] date NULL, 
	[ExpectedDeliveryDate] date NULL, 
	[CustomerPurchaseOrderNumber] varchar(8000) NULL, 
	[IsUndersupplyBackordered] bit NULL, 
	[Comments] varchar(8000) NULL, 
	[DeliveryInstructions] varchar(8000) NULL, 
	[InternalComments] varchar(8000) NULL, 
	[PickingCompletedWhen] datetime2(6) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);