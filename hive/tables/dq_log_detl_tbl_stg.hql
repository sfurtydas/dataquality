use ${hiveconf:dq_schema};
CREATE  TABLE dq_log_detl_stg
(dq_app_name string,
dq_prc_name string,
DQ_BATCH_START_DT string,
dq_check_id string,
dq_src_schema string,
dq_src_col string,
dq_chk_type  string,
dq_detl_hql string,
dq_threshold_per int,
DQ_TOT_REC_CNT decimal,
DQ_ERR_REC_CNT decimal,
dq_run_status string,
dq_run_tm string)
PARTITIONED BY (
DQ_SRC_TBL varchar(100),
DQ_BATCH_ID int)
stored as parquetfile ;




