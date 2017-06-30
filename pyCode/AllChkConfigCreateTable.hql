create table DQ_ALL_CHK_CONFIG
(
DQ_APP_NAME  varchar(100),
DQ_SRC_SCHEMA  varchar(100),
DQ_CHECK_ID  varchar(100),
Description varchar(100),
DQ_SRC_TBL  varchar(100),
DQ_SRC_COL  varchar(100),
PK_1 varchar(100),
PK_2 varchar(100),
PK_3 varchar(100),
PK_4 varchar(100),
PK_5 varchar(100),
NULL_CHK varchar(100),
NULL_FILTER_RULE varchar(100),
NULL_CHK_threshold_percentage varchar(100),
LEN_CHK varchar(100),
LEN_FILTER_RULE varchar(100),
MIN_LENGTH varchar(100),
MAX_LENGTH varchar(100),
LEN_CHK_threshold_percentage varchar(100),
LOV_CHK varchar(100),
LOV_FILTER_RULE varchar(100),
LOV_CHK_threshold_percentage varchar(100),
REF_CHK varchar(100),
REF_FILTER_RULE varchar(100),
CUSTOM_SQL varchar(100),
LOOKUP_TABLE_SCHEMA varchar(100),
LOOKUP_TABLE_NAME varchar(100),
LKP_TBL_KEY varchar(100),
CUST_SQL varchar(100),
CUST_SQL_TBL_NM varchar(100),
CUST_SQL_KEY varchar(100),
REF_CHK_threshold_percentage varchar(100),
DUP_CHK varchar(100),
DUP_FILTER_RULE varchar(100),
DUP_CHK_threshold_percentage varchar(100),
DataType_CHK varchar(100),
DATA_FILTER_RULE varchar(100),
DataType_CHK_threshold_percentage varchar(100),
CHK_created_dt timestamp
)
 row format delimited fields terminated by '~' ;		
 