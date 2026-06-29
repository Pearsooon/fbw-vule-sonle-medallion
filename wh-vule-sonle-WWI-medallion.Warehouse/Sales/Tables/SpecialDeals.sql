CREATE TABLE [Sales].[SpecialDeals] (

	[SpecialDealID] int NULL, 
	[StockItemID] int NULL, 
	[CustomerID] int NULL, 
	[BuyingGroupID] int NULL, 
	[CustomerCategoryID] int NULL, 
	[StockGroupID] int NULL, 
	[DealDescription] varchar(8000) NULL, 
	[StartDate] date NULL, 
	[EndDate] date NULL, 
	[DiscountAmount] decimal(18,2) NULL, 
	[DiscountPercentage] decimal(18,3) NULL, 
	[UnitPrice] decimal(18,2) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(6) NULL
);