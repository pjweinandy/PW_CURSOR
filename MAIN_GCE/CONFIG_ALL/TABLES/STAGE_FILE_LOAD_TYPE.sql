create or replace TABLE CONFIG_ALL.STAGE_FILE_LOAD_TYPE (
	BASE_NAME VARCHAR(16777216),
	DELTA_IND BOOLEAN,
	TEMPLATE_DELTA_SQL VARCHAR(16777216),
	DYNAMIC_RAW_IND BOOLEAN
);