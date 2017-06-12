from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
from pyspark.sql.functions import lit
from datetime import datetime
import datetime
import sys
# Three arguements are required to pass.
# 1st Arguement: Application NM.
# 2nd Arguement: Source Schema NM.
# 3rd Arguement: Source Tbl NM.

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Aml Markets DQ")
    sqlContext=HiveContext(sc)
    
    app_nm=sys.argv[1]
    dq_schema=sys.argv[2]
    
    #dq_schema='amlmkt_dq'
    dq_app_config=dq_schema+'.'+'dq_app_config'
    dq_log_detl_tbl_stg=dq_schema+'.'+'dq_log_detl_stg'
    dq_log_detl_tbl=dq_schema+'.'+'dq_log_detl'
    dq_chk_master=dq_schema+'.'+'dq_check_master'
    dq_reslt_detl_master_stg=dq_schema+'.'+'dq_reslt_detl_master_stg'
    dq_reslt_detl_master= dq_schema +'.'+'dq_reslt_detl_master'
    dq_reslt_summ_master=dq_schema+'.'+'dq_reslt_summ_master'
    print ("dq_schema : " ,dq_schema)
    print ("dq_app_config : " ,dq_app_config)
    print ("dq_reslt_summ_master : " ,dq_reslt_summ_master)
    print ("dq_chk_master : " ,dq_chk_master)
    print ("dq_reslt_detl_master_stg : " ,dq_reslt_detl_master_stg)
    print ("dq_reslt_detl_master : " ,dq_reslt_detl_master)
    print ("dq_log_detl_tbl_stg : " ,dq_log_detl_tbl_stg)
    print ("dq_log_detl_tbl : " ,dq_log_detl_tbl)
    
    
    # Inserting into dq_log_detl_tbl_stg  for check started..
    print ("dq_reslt_summ_master insert will start" )
    dfsql_sel_df_summ_master=sqlContext.sql('''SELECT DQ_APP_NAME,DQ_BATCH_ID,DQ_RUN_DT,DQ_CHECK_ID,DQ_TOT_REC_CNT,COUNT(1) err_cnt,DQ_THRSHOLD_FLAG,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL,DQ_CHK_TYPE,FROM_UNIXTIME(UNIX_TIMESTAMP(), 'YYYYMMDD') dq_load_dt FROM %s GROUP BY DQ_APP_NAME,DQ_BATCH_ID,DQ_RUN_DT,DQ_CHECK_ID,DQ_TOT_REC_CNT,DQ_THRSHOLD_FLAG,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL,DQ_CHK_TYPE ''' %(dq_reslt_detl_master_stg))
    dfsql_sel_df_summ_master_filter=dfsql_sel_df_summ_master.where(dfsql_sel_df_summ_master['DQ_APP_NAME'] == app_nm)
    dfsql_sel_df_summ_master_filter_cnt=dfsql_sel_df_summ_master_filter.count()
           
    if dfsql_sel_df_summ_master_filter_cnt == 0:
           print(" ***************** %s.%s has No error record to be loaded in summary Master.. ***************" %(dq_schema,dq_reslt_summ_master),file=sys.stderr)
           exit(-1)
    else:
         print(" ***************** %s.%s has %d error record to be loaded in summary Master.. ***************" %(dq_schema,dq_reslt_summ_master,dfsql_sel_df_summ_master_filter_cnt),file=sys.stderr)
         dfsql_sel_df_summ_master_filter.write.mode("append").saveAsTable(dq_reslt_summ_master)
    
    # Inserting into dq_log_detl_tbl_stg  for check started..
    dfsql_sel_df_detl_master=sqlContext.sql(''' select * from %s''' %(dq_reslt_detl_master_stg))
    dfsql_sel_df_detl_master_filter=dfsql_sel_df_detl_master.where(dfsql_sel_df_detl_master['DQ_APP_NAME'] == app_nm)
    dfsql_sel_df_detl_master_filter_cnt=dfsql_sel_df_detl_master_filter.count()
    
    if dfsql_sel_df_detl_master_filter_cnt == 0:
           print(" ***************** %s.%s has No error record to be loaded in summary Master.. ***************" %(dq_schema,dq_reslt_detl_master),file=sys.stderr)
           exit(-1)
    else:
         print(" ***************** %s.%s has %d error record to be loaded in summary Master.. ***************" %(dq_schema,dq_reslt_detl_master,dfsql_sel_df_detl_master_filter_cnt),file=sys.stderr)
         dfsql_sel_df_detl_master_filter.write.mode("append").saveAsTable(dq_reslt_detl_master)
 
    # Inserting into dq_log_detl_tbl_stg  for check started..
    dfsql_sel_df_log_detl=sqlContext.sql(''' select * from %s''' %(dq_log_detl_tbl_stg))
    dfsql_sel_df_log_detl_filter=dfsql_sel_df_log_detl.where(dfsql_sel_df_log_detl['DQ_APP_NAME'] == app_nm)
    dfsql_sel_df_log_detl_filter_cnt=dfsql_sel_df_log_detl_filter.count()
    
    
    if dfsql_sel_df_log_detl_filter_cnt == 0:
           print(" ***************** %s.%s has No error record to be loaded in summary Master.. ***************" %(dq_schema,dq_log_detl_tbl),file=sys.stderr)
           exit(-1)
    else:
         print(" ***************** %s.%s has %d error record to be loaded in summary Master.. ***************" %(dq_schema,dq_log_detl_tbl,dfsql_sel_df_log_detl_filter_cnt),file=sys.stderr)
         dfsql_sel_df_log_detl_filter_cnt.write.mode("append").saveAsTable(dq_log_detl_tbl)
        