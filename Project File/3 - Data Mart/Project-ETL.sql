------------------------------------------------------------
-- Create DW database
------------------------------------------------------------

DROP DATABASE IF EXISTS UsaCarriers_DW;
GO
CREATE DATABASE UsaCarriers_DW;
GO
USE UsaCarriers_DW
GO

------------------------------------------------------------
-- Create Fact and Dim table
------------------------------------------------------------

-- DimAircraft

DROP TABLE IF EXISTS dbo.DimAircraft;
GO
CREATE TABLE dbo.DimAircraft(
	AIRCRAFT_KEY INT NOT NULL,
	AIRCRAFT_GROUP NVARCHAR(50) NULL,
	AIRCRAFT_TYPE INT NULL,
	AIRCRAFT_CONFIG NVARCHAR(50) NULL,
    CONSTRAINT PK_DimAircraft PRIMARY KEY CLUSTERED ( AIRCRAFT_KEY )
);

-- DimAirport

DROP TABLE IF EXISTS dbo.DimAirport;
GO
CREATE TABLE dbo.DimAirport(
	AIRPORT_KEY INT NOT NULL,
	AIRPORT_SEQ_ID INT NULL,
	CITY_MARKET_ID INT NULL,
	IATACODE NVARCHAR(50) NULL,
	CITY_NAME NVARCHAR(50) NULL,
	STATE_ABR NVARCHAR(50) NULL,
	STATE_NM NVARCHAR(50) NULL,
    CONSTRAINT PK_DimAirport PRIMARY KEY CLUSTERED ( AIRPORT_KEY )
);

-- DimCarrier

DROP TABLE IF EXISTS dbo.DimCarrier;
GO
CREATE TABLE dbo.DimCarrier(
	CARRIER_KEY INT NOT NULL,
	AIRLINE_ID INT NULL,
	UNIQUE_CARRIER_NAME NVARCHAR(100) NULL,
	REGION NVARCHAR(50) NULL,
	STARTDATE DATE NOT NULL,
	ENDDATE DATE NULL,
    CONSTRAINT PK_DimCarrier PRIMARY KEY CLUSTERED ( CARRIER_KEY )
);

-- DimFlightdate

DROP TABLE IF EXISTS dbo.DimFlightdate;
GO
CREATE TABLE dbo.DimFlightdate(
	FLIGHTDATE_KEY INT NOT NULL,
	YEAR INT NULL,
	QUARTER NVARCHAR(50) NULL,
	MONTH NVARCHAR(50) NULL,
    CONSTRAINT PK_DimFlightdate PRIMARY KEY CLUSTERED ( FLIGHTDATE_KEY )
);

-- FactSummary

DROP TABLE IF EXISTS dbo.FactSummary;
GO
CREATE TABLE dbo.FactSummary(
	-- FK part
	CARRIER_KEY INT NOT NULL,
	ORIGIN_AIRPORT_KEY INT NOT NULL,
	DEST_AIRPORT_KEY INT NOT NULL,
	AIRCRAFT_KEY INT NOT NULL,
	FLIGHTDATE_KEY INT NOT NULL,
	-- Payload part
	PAYLOAD INT NOT NULL,
	FREIGHT INT NOT NULL,
	MAIL INT NOT NULL,
	PAYLOAD_RATE DECIMAL(10, 4) NOT NULL,
	-- Passenger part
	SEATS INT NOT NULL,
	PASSENGERS INT NOT NULL,
	PASSENGER_RATE DECIMAL(10, 4) NOT NULL,
	-- Flight time part
	RAMP_TO_RAMP INT NOT NULL,
	AIR_TIME INT NOT NULL,
	FLIGHT_EFFICIENCY DECIMAL(10, 4) NOT NULL
	-- Define FK
    CONSTRAINT FK_FactSummary_DimCarrier FOREIGN KEY(CARRIER_KEY) REFERENCES dbo.DimCarrier (CARRIER_KEY),
	CONSTRAINT FK_FactSummary_DimOriginAirport FOREIGN KEY(ORIGIN_AIRPORT_KEY) REFERENCES dbo.DimAirport (AIRPORT_KEY),
	CONSTRAINT FK_FactSummary_DimDestAirport FOREIGN KEY(DEST_AIRPORT_KEY) REFERENCES dbo.DimAirport (AIRPORT_KEY),
	CONSTRAINT FK_FactSummary_DimAircraft FOREIGN KEY(AIRCRAFT_KEY) REFERENCES dbo.DimAircraft (AIRCRAFT_KEY),
	CONSTRAINT FK_FactSummary_DimDate FOREIGN KEY(FLIGHTDATE_KEY) REFERENCES dbo.DimFlightdate (FLIGHTDATE_KEY)
);
GO

------------------------------------------------------------
-- Create Stage table
------------------------------------------------------------

-- Aircraft_Stage

DROP TABLE IF EXISTS dbo.Aircraft_Stage;
GO
CREATE TABLE dbo.Aircraft_Stage (
	AIRCRAFT_GROUP NVARCHAR(50),
	AIRCRAFT_TYPE INT,
	AIRCRAFT_CONFIG NVARCHAR(50)
);

DROP PROCEDURE IF EXISTS dbo.Aircraft_Extract;
GO
CREATE PROCEDURE dbo.Aircraft_Extract
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @RowCt INT;

	TRUNCATE TABLE dbo.Aircraft_Stage;

INSERT INTO dbo.Aircraft_Stage (
	AIRCRAFT_GROUP,
	AIRCRAFT_TYPE,
	AIRCRAFT_CONFIG
	)
	SELECT at.AIRCRAFT_GROUP,
		   at.AIRCRAFT_TYPE,
	       at.AIRCRAFT_CONFIG
	FROM UsaCarriers.dbo.Aircraft at

	SET @RowCt = @@ROWCOUNT;
	IF @RowCt = 0
	BEGIN;
		THROW 50001 ,'No records found. Check with source system.', 1;
	END;
END;

Exec dbo.Aircraft_Extract

-- Airport_Stage

DROP TABLE IF EXISTS dbo.Airport_Stage;
GO
CREATE TABLE dbo.Airport_Stage (
	AIRPORT_SEQ_ID INT,
	CITY_MARKET_ID INT,
	IATACODE NVARCHAR(50),
	CITY_NAME NVARCHAR(50),
	STATE_ABR NVARCHAR(50),
	STATE_NM NVARCHAR(50)
);

DROP PROCEDURE IF EXISTS dbo.Airport_Extract;
GO
CREATE PROCEDURE dbo.Airport_Extract
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @RowCt INT;

	TRUNCATE TABLE dbo.Airport_Stage;

INSERT INTO dbo.Airport_Stage (
	AIRPORT_SEQ_ID,
	CITY_MARKET_ID,
	IATACODE,
	CITY_NAME,
	STATE_ABR,
	STATE_NM
	)
	SELECT at.AIRPORT_SEQ_ID,
		   at.CITY_MARKET_ID,
		   at.IATACODE,
		   at.CITY_NAME,
		   se.STATE_ABR,
		   se.STATE_NM
	FROM UsaCarriers.dbo.Airport at
	LEFT JOIN UsaCarriers.dbo.State se
		ON at.StateID = se.StateID

	SET @RowCt = @@ROWCOUNT;
	IF @RowCt = 0
	BEGIN;
		THROW 50001 ,'No records found. Check with source system.', 1;
	END;
END;

Exec dbo.Airport_Extract

-- Carrier_Stage

DROP TABLE IF EXISTS dbo.Carrier_Stage;
GO
CREATE TABLE dbo.Carrier_Stage (
	AIRLINE_ID INT,
	UNIQUE_CARRIER_NAME NVARCHAR(100),
	REGION NVARCHAR(50)
);

DROP PROCEDURE IF EXISTS dbo.Carrier_Extract;
GO
CREATE PROCEDURE dbo.Carrier_Extract
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @RowCt INT;

	TRUNCATE TABLE dbo.Carrier_Stage;

INSERT INTO dbo.Carrier_Stage (
	AIRLINE_ID,
	UNIQUE_CARRIER_NAME,
	REGION
	)
	SELECT cr.AIRLINE_ID,
		   cr.UNIQUE_CARRIER_NAME,
		   cr.REGION
	FROM UsaCarriers.dbo.Carrier cr

	SET @RowCt = @@ROWCOUNT;
	IF @RowCt = 0
	BEGIN;
		THROW 50001 ,'No records found. Check with source system.', 1;
	END;
END;

Exec dbo.Carrier_Extract

-- Flightdate_Stage

DROP TABLE IF EXISTS dbo.Flightdate_Stage;
GO
CREATE TABLE dbo.Flightdate_Stage (
	YEAR INT,
	QUARTER NVARCHAR(50),
	MONTH NVARCHAR(50)
);

DROP PROCEDURE IF EXISTS dbo.Flightdate_Extract;
GO
CREATE PROCEDURE dbo.Flightdate_Extract
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @RowCt INT;

	TRUNCATE TABLE dbo.Flightdate_Stage;

INSERT INTO dbo.Flightdate_Stage (
	YEAR,
	QUARTER,
	MONTH
	)
	SELECT fe.YEAR,
		   fe.QUARTER,
		   fe.MONTH
	FROM UsaCarriers.dbo.Flightdate fe

	SET @RowCt = @@ROWCOUNT;
	IF @RowCt = 0
	BEGIN;
		THROW 50001 ,'No records found. Check with source system.', 1;
	END;
END;

Exec dbo.Flightdate_Extract

-- Summary_Stage

DROP TABLE IF EXISTS dbo.Summary_Stage;
GO
CREATE TABLE dbo.Summary_Stage (
	-- FK part
	CARRIER_KEY INT,
	ORIGIN_AIRPORT_KEY INT,
	DEST_AIRPORT_KEY INT,
	AIRCRAFT_KEY INT,
	FLIGHTDATE_KEY INT,
	-- Payload part
	PAYLOAD INT,
	FREIGHT INT,
	MAIL INT,
	-- Passenger part
	SEATS INT,
	PASSENGERS INT,
	-- Flight time part
	RAMP_TO_RAMP INT,
	AIR_TIME INT
);

DROP PROCEDURE IF EXISTS dbo.Summary_Extract;
GO
CREATE PROCEDURE dbo.Summary_Extract
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @RowCt INT;

	TRUNCATE TABLE dbo.Summary_Stage;

INSERT INTO dbo.Summary_Stage (
	-- FK part
	CARRIER_KEY,
	ORIGIN_AIRPORT_KEY,
	DEST_AIRPORT_KEY,
	AIRCRAFT_KEY,
	FLIGHTDATE_KEY,
	-- Payload part
	PAYLOAD,
	FREIGHT,
	MAIL,
	-- Passenger part
	SEATS,
	PASSENGERS,
	-- Flight time part
	RAMP_TO_RAMP,
	AIR_TIME
	)
	SELECT sy.CarrierID,
		   sy.ORIGIN_AirportID,
	       sy.DEST_AirportID,
	       sy.AircraftID,
	       sy.FlightdateID,
		   sy.PAYLOAD,
		   sy.FREIGHT,
		   sy.MAIL,
		   sy.SEATS,
		   sy.PASSENGERS,
		   sy.RAMP_TO_RAMP,
		   sy.AIR_TIME
	FROM UsaCarriers.dbo.Summary sy

	SET @RowCt = @@ROWCOUNT;
	IF @RowCt = 0
	BEGIN;
		THROW 50001 ,'No records found. Check with source system.', 1;
	END;
END;

Exec dbo.Summary_Extract

------------------------------------------------------------
-- Create Preload table
------------------------------------------------------------

-- Aircraft_Preload

DROP TABLE IF EXISTS dbo.Aircraft_Preload;
GO
CREATE TABLE dbo.Aircraft_Preload (
	AIRCRAFT_KEY INT NOT NULL,
	AIRCRAFT_GROUP NVARCHAR(50) NULL,
	AIRCRAFT_TYPE INT NULL,
	AIRCRAFT_CONFIG NVARCHAR(50) NULL,
    CONSTRAINT PK_Aircraft_Preload PRIMARY KEY CLUSTERED ( AIRCRAFT_KEY )
);

DROP SEQUENCE IF EXISTS dbo.AIRCRAFT_KEY;
GO
CREATE SEQUENCE dbo.AIRCRAFT_KEY START WITH 1;

DROP PROCEDURE IF EXISTS dbo.Aircraft_Transform;
GO
CREATE PROCEDURE dbo.Aircraft_Transform
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	TRUNCATE TABLE dbo.Aircraft_Preload;
	BEGIN TRANSACTION;

	INSERT INTO dbo.Aircraft_Preload
	SELECT NEXT VALUE FOR dbo.AIRCRAFT_KEY AS AIRCRAFT_KEY,
		st.AIRCRAFT_GROUP,
		st.AIRCRAFT_TYPE,
		st.AIRCRAFT_CONFIG
	FROM dbo.Aircraft_Stage st
	WHERE NOT EXISTS ( SELECT 1
						FROM dbo.DimAircraft dm
						WHERE st.AIRCRAFT_GROUP = dm.AIRCRAFT_GROUP
								AND st.AIRCRAFT_TYPE = dm.AIRCRAFT_TYPE
								AND st.AIRCRAFT_CONFIG = dm.AIRCRAFT_CONFIG);
	INSERT INTO dbo.Aircraft_Preload
	SELECT  dm.AIRCRAFT_KEY,
			dm.AIRCRAFT_GROUP,
			dm.AIRCRAFT_TYPE,
			dm.AIRCRAFT_CONFIG
	FROM dbo.Aircraft_Stage st
	JOIN dbo.DimAircraft dm
		ON st.AIRCRAFT_GROUP = dm.AIRCRAFT_GROUP
		AND st.AIRCRAFT_TYPE = dm.AIRCRAFT_TYPE
		AND st.AIRCRAFT_CONFIG = dm.AIRCRAFT_CONFIG;
COMMIT TRANSACTION;
END;

Exec dbo.Aircraft_Transform

-- Airport_Preload

DROP TABLE IF EXISTS dbo.Airport_Preload;
GO
CREATE TABLE dbo.Airport_Preload (
	AIRPORT_KEY INT NOT NULL,
	AIRPORT_SEQ_ID INT NULL,
	CITY_MARKET_ID INT NULL,
	IATACODE NVARCHAR(50) NULL,
	CITY_NAME NVARCHAR(50) NULL,
	STATE_ABR NVARCHAR(50) NULL,
	STATE_NM NVARCHAR(50) NULL,
    CONSTRAINT PK_Airport_Preload PRIMARY KEY CLUSTERED ( AIRPORT_KEY )
);

DROP SEQUENCE IF EXISTS dbo.AIRPORT_KEY;
GO
CREATE SEQUENCE dbo.AIRPORT_KEY START WITH 1;

DROP PROCEDURE IF EXISTS dbo.Airport_Transform;
GO
CREATE PROCEDURE dbo.Airport_Transform
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	TRUNCATE TABLE dbo.Airport_Preload;
	BEGIN TRANSACTION;

	INSERT INTO dbo.Airport_Preload
	SELECT NEXT VALUE FOR dbo.AIRPORT_KEY AS AIRPORT_KEY,
		st.AIRPORT_SEQ_ID,
		st.CITY_MARKET_ID,
		st.IATACODE,
		st.CITY_NAME,
		st.STATE_ABR,
		st.STATE_NM
	FROM dbo.Airport_Stage st
	WHERE NOT EXISTS ( SELECT 1
						FROM dbo.DimAirport dm
						WHERE st.IATACODE = dm.IATACODE );
	INSERT INTO dbo.Airport_Preload
	SELECT  dm.AIRPORT_KEY,
			dm.AIRPORT_SEQ_ID,
			dm.CITY_MARKET_ID,
			dm.IATACODE,
			dm.CITY_NAME,
			dm.STATE_ABR,
			dm.STATE_NM
	FROM dbo.Airport_Stage st
	JOIN dbo.DimAirport dm
		ON st.IATACODE = dm.IATACODE;
COMMIT TRANSACTION;
END;

Exec dbo.Airport_Transform

-- Carrier_Preload

DROP TABLE IF EXISTS dbo.Carrier_Preload;
GO
CREATE TABLE dbo.Carrier_Preload (
	CARRIER_KEY INT NOT NULL,
	AIRLINE_ID INT NULL,
	UNIQUE_CARRIER_NAME NVARCHAR(100) NULL,
	REGION NVARCHAR(50) NULL,
	STARTDATE DATE NOT NULL,
	ENDDATE DATE NULL,
    CONSTRAINT PK_Carrier_Preload PRIMARY KEY CLUSTERED ( CARRIER_KEY )
);

DROP SEQUENCE IF EXISTS dbo.CARRIER_KEY;
GO
CREATE SEQUENCE dbo.CARRIER_KEY START WITH 1;

DROP PROCEDURE IF EXISTS dbo.Carrier_Transform;
GO
CREATE PROCEDURE dbo.Carrier_Transform
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	TRUNCATE TABLE dbo.Carrier_Preload;

	DECLARE @StartDate DATE = GETDATE();
	DECLARE @EndDate DATE = DATEADD(dd,-1,GETDATE());

	BEGIN TRANSACTION;

	INSERT INTO dbo.Carrier_Preload
	SELECT NEXT VALUE FOR dbo.CARRIER_KEY AS CARRIER_KEY,
		st.AIRLINE_ID,
		st.UNIQUE_CARRIER_NAME,
		st.REGION,
		@StartDate,
		NULL
	FROM dbo.Carrier_Stage st
	WHERE NOT EXISTS ( SELECT 1
						FROM dbo.DimCarrier dm
						WHERE st.AIRLINE_ID = dm.AIRLINE_ID
							AND st.REGION = dm.REGION);
	INSERT INTO dbo.Carrier_Preload
	SELECT  dm.CARRIER_KEY,
			dm.AIRLINE_ID,
			dm.UNIQUE_CARRIER_NAME,
			dm.REGION,
			dm.STARTDATE,
			@EndDate
	FROM dbo.Carrier_Stage st
	JOIN dbo.DimCarrier dm
		ON st.AIRLINE_ID = dm.AIRLINE_ID
		AND st.REGION = dm.REGION;
COMMIT TRANSACTION;
END;

Exec dbo.Carrier_Transform

-- Flightdate_Preload

DROP TABLE IF EXISTS dbo.Flightdate_Preload;
GO
CREATE TABLE dbo.Flightdate_Preload (
	FLIGHTDATE_KEY INT NOT NULL,
	YEAR INT NULL,
	QUARTER NVARCHAR(50) NULL,
	MONTH NVARCHAR(50) NULL,
    CONSTRAINT PK_Flightdate_Preload PRIMARY KEY CLUSTERED ( FLIGHTDATE_KEY )
);

DROP SEQUENCE IF EXISTS dbo.FLIGHTDATE_KEY;
GO
CREATE SEQUENCE dbo.FLIGHTDATE_KEY START WITH 1;

DROP PROCEDURE IF EXISTS dbo.Flightdate_Transform;
GO
CREATE PROCEDURE dbo.Flightdate_Transform
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	TRUNCATE TABLE dbo.Flightdate_Preload;
	BEGIN TRANSACTION;

	INSERT INTO dbo.Flightdate_Preload
	SELECT NEXT VALUE FOR dbo.FLIGHTDATE_KEY AS FLIGHTDATE_KEY,
		st.YEAR,
		st.QUARTER,
		st.MONTH
	FROM dbo.Flightdate_Stage st
	WHERE NOT EXISTS ( SELECT 1
						FROM dbo.DimFlightdate dm
						WHERE st.MONTH = dm.MONTH );
	INSERT INTO dbo.Flightdate_Preload
	SELECT  dm.FLIGHTDATE_KEY,
			dm.YEAR,
			dm.QUARTER,
			dm.MONTH
	FROM dbo.Flightdate_Stage st
	JOIN dbo.DimFlightdate dm
		ON st.MONTH = dm.MONTH;
COMMIT TRANSACTION;
END;

Exec dbo.Flightdate_Transform

-- Summary_Preload

DROP TABLE IF EXISTS dbo.Summary_Preload;
GO
CREATE TABLE dbo.Summary_Preload (
	-- FK part
	CARRIER_KEY INT NOT NULL,
	ORIGIN_AIRPORT_KEY INT NOT NULL,
	DEST_AIRPORT_KEY INT NOT NULL,
	AIRCRAFT_KEY INT NOT NULL,
	FLIGHTDATE_KEY INT NOT NULL,
	-- Payload part
	PAYLOAD INT NOT NULL,
	FREIGHT INT NOT NULL,
	MAIL INT NOT NULL,
	PAYLOAD_RATE DECIMAL(10, 4) NOT NULL,
	-- Passenger part
	SEATS INT NOT NULL,
	PASSENGERS INT NOT NULL,
	PASSENGER_RATE DECIMAL(10, 4) NOT NULL,
	-- Flight time part
	RAMP_TO_RAMP INT NOT NULL,
	AIR_TIME INT NOT NULL,
	FLIGHT_EFFICIENCY DECIMAL(10, 4) NOT NULL
);

DROP PROCEDURE IF EXISTS dbo.Summary_Transform;
GO
CREATE PROCEDURE dbo.Summary_Transform
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	TRUNCATE TABLE dbo.Summary_Preload;

	INSERT INTO dbo.Summary_Preload
	SELECT  -- FK part
			car.CARRIER_KEY,
			apt1.AIRPORT_KEY,
			apt2.AIRPORT_KEY,
			act.AIRCRAFT_KEY,
			fli.FLIGHTDATE_KEY,
			-- Payload part
			sta.PAYLOAD,
			sta.FREIGHT,
			sta.MAIL,
				--((sta.FREIGHT+sta.MAIL)/sta.PAYLOAD) AS PAYLOAD_RATE,
			CASE
				WHEN sta.PAYLOAD <> 0 THEN CONVERT(DECIMAL(10,4),ROUND((sta.FREIGHT+sta.MAIL)*1.00/sta.PAYLOAD ,4))
				ELSE 0
			END AS PAYLOAD_RATE,
			-- Passenger part
			sta.SEATS,
			sta.PASSENGERS,
				--(sta.PASSENGERS/sta.SEATS)AS PASSENGER_RATE,
			CASE
				WHEN sta.SEATS <> 0 THEN CONVERT(DECIMAL(10,4),ROUND(sta.PASSENGERS*1.00/sta.SEATS ,4))
				ELSE 0
			END AS PASSENGER_RATE,
			-- Flight time part
			sta.RAMP_TO_RAMP,
			sta.AIR_TIME,
				--(sta.AIR_TIME/sta.RAMP_TO_RAMP) AS FLIGHT_EFFICIENCY
			CASE
				WHEN sta.RAMP_TO_RAMP <> 0 THEN CONVERT(DECIMAL(10,4),ROUND(sta.AIR_TIME*1.00/sta.RAMP_TO_RAMP ,4))
				ELSE 0
			END AS FLIGHT_EFFICIENCY
	FROM dbo.Summary_Stage sta
	JOIN dbo.Aircraft_Preload act
		ON sta.AIRCRAFT_KEY = act.AIRCRAFT_KEY
	JOIN dbo.Airport_Preload apt1
		ON sta.ORIGIN_AIRPORT_KEY = apt1.AIRPORT_KEY
	JOIN dbo.Airport_Preload apt2
		ON sta.DEST_AIRPORT_KEY = apt2.AIRPORT_KEY
	JOIN dbo.Carrier_Preload car
		ON sta.CARRIER_KEY = car.CARRIER_KEY
	JOIN dbo.Flightdate_Preload fli
		ON sta.FLIGHTDATE_KEY = fli.FLIGHTDATE_KEY
	GROUP BY act.AIRCRAFT_KEY,
			 apt1.AIRPORT_KEY,
			 apt2.AIRPORT_KEY,
			 car.CARRIER_KEY,
			 fli.FLIGHTDATE_KEY,
			 sta.PAYLOAD,
			 sta.FREIGHT,
			 sta.MAIL,
			 sta.SEATS,
			 sta.PASSENGERS,
			 sta.RAMP_TO_RAMP,
			 sta.AIR_TIME
END;

Exec dbo.Summary_Transform

------------------------------------------------------------
-- Create ETL Loads
------------------------------------------------------------

-- Aircraft

DROP PROCEDURE IF EXISTS dbo.Aircraft_Load;
GO
CREATE PROCEDURE dbo.Aircraft_Load
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	BEGIN TRANSACTION;

	DELETE dim
	FROM dbo.DimAircraft dim
	JOIN dbo.Aircraft_Preload pre
		ON dim.AIRCRAFT_KEY = pre.AIRCRAFT_KEY;

	INSERT INTO dbo.DimAircraft
	SELECT *
	FROM dbo.Aircraft_Preload;
	
	COMMIT TRANSACTION;
END;

Exec dbo.Aircraft_Load

-- Airport

DROP PROCEDURE IF EXISTS dbo.Airport_Load;
GO
CREATE PROCEDURE dbo.Airport_Load
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	BEGIN TRANSACTION;

	DELETE dim
	FROM dbo.DimAirport dim
	JOIN dbo.Airport_Preload pre
		ON dim.AIRPORT_KEY = pre.AIRPORT_KEY;

	INSERT INTO dbo.DimAirport
	SELECT *
	FROM dbo.Airport_Preload;
	
	COMMIT TRANSACTION;
END;

Exec dbo.Airport_Load

-- Carrier

DROP PROCEDURE IF EXISTS dbo.Carrier_Load;
GO
CREATE PROCEDURE dbo.Carrier_Load
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	BEGIN TRANSACTION;

	DELETE dim
	FROM dbo.DimCarrier dim
	JOIN dbo.Carrier_Preload pre
		ON dim.CARRIER_KEY = pre.CARRIER_KEY;

	INSERT INTO dbo.DimCarrier
	SELECT *
	FROM dbo.Carrier_Preload;
	
	COMMIT TRANSACTION;
END;

Exec dbo.Carrier_Load

-- Flightdate

DROP PROCEDURE IF EXISTS dbo.Flightdate_Load;
GO
CREATE PROCEDURE dbo.Flightdate_Load
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	BEGIN TRANSACTION;

	DELETE dim
	FROM dbo.DimFlightdate dim
	JOIN dbo.Flightdate_Preload pre
		ON dim.FLIGHTDATE_KEY = pre.FLIGHTDATE_KEY;

	INSERT INTO dbo.DimFlightdate
	SELECT *
	FROM dbo.Flightdate_Preload;
	
	COMMIT TRANSACTION;
END;

Exec dbo.Flightdate_Load

-- Summary

DROP PROCEDURE IF EXISTS dbo.Summary_Load;
GO
CREATE PROCEDURE dbo.Summary_Load
AS
BEGIN;

	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	INSERT INTO dbo.FactSummary
	SELECT *
	FROM dbo.Summary_Preload;

END;

Exec dbo.Summary_Load