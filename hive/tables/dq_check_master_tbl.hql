use ${hiveconf:dq_schema};
CREATE TABLE DQ_CHECK_MASTER
(
dq_app_name string,
dq_check_id string,
dq_check_desc string,
dq_src_schema string,
dq_src_tbl string,
dq_src_col string,
dq_threshold_per int,
dq_detl_hql string,
dq_chk_type  string,
dq_chk_created_dt string)
row format delimited fields terminated by '~';


