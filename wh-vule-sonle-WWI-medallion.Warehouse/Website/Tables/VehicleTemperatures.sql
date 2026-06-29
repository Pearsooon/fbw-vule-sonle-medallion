CREATE TABLE [Website].[VehicleTemperatures] (

	[VehicleTemperatureID] bigint NULL, 
	[VehicleRegistration] varchar(8000) NULL, 
	[ChillerSensorNumber] int NULL, 
	[RecordedWhen] datetime2(6) NULL, 
	[Temperature] decimal(10,2) NULL, 
	[FullSensorData] varchar(8000) NULL
);