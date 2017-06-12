use ${hiveconf:dq_schema};
CREATE  TABLE DQ_RESLT_DETL_MASTER_STG(
DQ_CHECK_ID varchar(100),
PK_COLUMN_NM varchar(100),
PK_1 varchar(100),
PK_2 varchar(100),
PK_3 varchar(100),
PK_4 varchar(100),
PK_5 varchar(100),
PK_6 varchar(100),
PK_7 varchar(100),
PK_8 varchar(100),
ERR_VALUE varchar(100),
DQ_TOT_REC_CNT DECIMAL,
DQ_ERR_REC_CNT DECIMAL,
DQ_THRSHOLD_FLAG INT,
DQ_RUN_DT varchar(100),
DQ_APP_NAME varchar(100),
DQ_SRC_SCHEMA varchar(100),
DQ_SRC_COL varchar(100),
DQ_CHK_TYPE varchar(100),
DQ_RUN_TIME varchar(100))
PARTITIONED BY (
DQ_SRC_TBL varchar(100),
DQ_BATCH_ID bigint)
stored as parquetfile ;
