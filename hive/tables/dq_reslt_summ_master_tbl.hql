use ${hiveconf:dq_schema};
CREATE  TABLE DQ_RESLT_SUMM_MASTER(                                                
DQ_APP_NAME varchar(100),
DQ_RUN_DT varchar(100),
DQ_CHECK_ID varchar(100),
DQ_CHK_TYPE varchar(100),
DQ_TOT_REC_CNT DECIMAL,
DQ_ERR_REC_CNT DECIMAL,
DQ_THRSHOLD_FLAG INT,
DQ_SRC_SCHEMA varchar(100),
DQ_SRC_TBL varchar(100),
DQ_SRC_COL varchar(100),
DQ_RUN_TM varchar(100)) 
PARTITIONED BY (
DQ_BATCH_ID bigint)
stored as parquetfile ;






