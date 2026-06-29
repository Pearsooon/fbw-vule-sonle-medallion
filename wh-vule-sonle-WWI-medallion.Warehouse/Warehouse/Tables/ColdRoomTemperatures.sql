CREATE TABLE [Warehouse].[ColdRoomTemperatures] (

	[ColdRoomTemperatureID] bigint NULL, 
	[ColdRoomSensorNumber] int NULL, 
	[RecordedWhen] datetime2(6) NULL, 
	[Temperature] decimal(10,2) NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);