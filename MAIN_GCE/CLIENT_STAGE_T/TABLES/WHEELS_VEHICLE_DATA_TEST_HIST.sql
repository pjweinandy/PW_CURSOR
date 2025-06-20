create or replace TABLE CLIENT_STAGE_T.WHEELS_VEHICLE_DATA_TEST_HIST (
	CALC_DT DATE,
	ASSET_ID VARCHAR(16777216),
	VEHICLE_TYPE_DESCRIPTION VARCHAR(16777216),
	MAKE VARCHAR(16777216),
	MODEL VARCHAR(16777216),
	INTEREST_RATE NUMBER(10,8),
	RESERVE_RATE NUMBER(10,8),
	FIXED_FLOAT VARCHAR(16777216),
	BASE_INDEX VARCHAR(16777216),
	FUNDING_DESCRIPTION VARCHAR(16777216),
	IN_SERVICE_DATE TIMESTAMP_NTZ(9),
	MANAGEMENT_FEE_RATE_PCT NUMBER(10,8),
	MANAGEMENT_FEE_AMT NUMBER(20,8),
	TERM_DT VARCHAR(16777216),
	SOLD_DT VARCHAR(16777216),
	ORIGINAL_TERM_NUM NUMBER(38,0),
	MONTHS_IN_SERVICE_NUM NUMBER(38,0),
	DISPOSAL_INVOICE_DATE VARCHAR(16777216),
	GVWR NUMBER(38,0),
	TITLE_RECV_DT VARCHAR(16777216),
	OWNERCODE VARCHAR(16777216),
	LEASETYPE VARCHAR(16777216),
	ACQUISITIONTYPE VARCHAR(16777216),
	ENGINEFUELTYPE VARCHAR(16777216),
	GARAGESTATE VARCHAR(16777216),
	VIN VARCHAR(16777216),
	MODELYEAR NUMBER(4,0),
	DELDATE TIMESTAMP_NTZ(9)
);