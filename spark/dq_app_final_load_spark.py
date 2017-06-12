from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import rowNumber
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
    #src_schema=sys.argv[3]
   
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
   
    sqlContext.setConf("hive.exec.dynamic.partition ","true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
    dfsql_sel_batch_id=sqlContext.sql("select max(dq_batch_id) batch_id from %s where dq_src_tbl='BATCH_EXE_STATS' and DQ_APP_NAME='%s'  " %(dq_log_detl_tbl_stg,app_nm))
    dfsql_sel_batch_id.show()
    dfsql_sel_nxt_batch_id_rdd_list=dfsql_sel_batch_id.rdd.map(list)
    for line in dfsql_sel_nxt_batch_id_rdd_list.collect():
        print ("all very very  good")
        batch_id=line[0]
        print ("batch_id : " ,batch_id)
    # Inserting into dq_log_detl_tbl_stg  for check started..
    dfsql_sel_df_log_detl=sqlContext.sql(" select DQ_APP_NAME,DQ_PRC_NAME,DQ_BATCH_START_DT,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL,DQ_CHK_TYPE,DQ_RUN_STATUS,DQ_TOT_REC_CNT,DQ_ERR_REC_CNT,DQ_DETL_HQL,DQ_THRESHOLD_PER ,DQ_RUN_TM, dq_batch_id  from %s where dq_batch_id=%d  " %(dq_log_detl_tbl_stg,batch_id))
    #dfsql_sel_df_log_detl.show()
    dfsql_sel_df_log_detl_filter_cnt=dfsql_sel_df_log_detl.count()
   
    
    if dfsql_sel_df_log_detl_filter_cnt == 0:
           print(" ***************** %s.%s has No error record to be loaded in dq_log_detl_tbl.. ***************" %(dq_schema,dq_log_detl_tbl),file=sys.stderr)
           exit(-1)
    else:
        print(" ***************** %s.%s has %d error record to be loaded in dq_log_detl_tbl.. ***************" %(dq_schema,dq_log_detl_tbl,dfsql_sel_df_log_detl_filter_cnt),file=sys.stderr)
        #dfsql_sel_df_log_detl.write.mode("append").partitionBy('dq_batch_id').saveAsTable(dq_log_detl_tbl)
        #dfsql_sel_df_log_detl.show()
       
    dfsql_sel_df_detl_master=sqlContext.sql( "select DQ_APP_NAME,DQ_RUN_DT,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL,DQ_CHECK_ID,DQ_CHK_TYPE,DQ_TOT_REC_CNT,DQ_ERR_REC_CNT,dq_thrshold_flag,pk_column_nm,pk_1,pk_2,pk_3,pk_4,pk_5,pk_6,pk_7,pk_8,err_value,dq_run_time,dq_batch_id from %s where dq_batch_id=%d " %(dq_reslt_detl_master_stg,batch_id))
    dfsql_sel_df_detl_master_cnt=dfsql_sel_df_detl_master.count()
   
    if dfsql_sel_df_detl_master_cnt == 0:
           print(" ***************** %s.%s has No error record to be loaded in summary Master.. ***************" %(dq_schema,dq_reslt_detl_master),file=sys.stderr)
           exit(-1)
    else:
         print(" ***************** %s.%s has %d error record to be loaded in summary Master.. ***************" %(dq_schema,dq_reslt_detl_master,dfsql_sel_df_detl_master_cnt),file=sys.stderr)
         #dfsql_sel_df_detl_master.show()
         dfsql_sel_df_detl_master.write.mode("append").partitionBy('dq_batch_id').saveAsTable(dq_reslt_detl_master)
         dfsql_sel_df_detl_master_sel=dfsql_sel_df_detl_master.select('DQ_APP_NAME','DQ_RUN_DT','DQ_SRC_SCHEMA','DQ_SRC_TBL','DQ_SRC_COL','DQ_CHECK_ID','DQ_CHK_TYPE','DQ_TOT_REC_CNT','DQ_ERR_REC_CNT','DQ_THRSHOLD_FLAG','DQ_RUN_TIME','dq_batch_id')
         data_with_rank=dfsql_sel_df_detl_master_sel.withColumn("rank",rowNumber().over(Window.partitionBy('DQ_APP_NAME','DQ_SRC_SCHEMA','DQ_SRC_TBL','DQ_CHECK_ID','dq_batch_id').orderBy(dfsql_sel_df_detl_master_sel["DQ_APP_NAME"].desc())))
         dfsql_sel_df_summ_master_sel=data_with_rank.filter(data_with_rank["rank"] == 1).drop('rank')
         #dfsql_sel_df_summ_master_sel.show()
         dfsql_sel_df_summ_master=sqlContext.sql("select DQ_APP_NAME,DQ_RUN_DT,DQ_CHECK_ID,DQ_CHK_TYPE,DQ_TOT_REC_CNT,DQ_ERR_REC_CNT,DQ_THRSHOLD_FLAG,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL,DQ_RUN_TIME,dq_batch_id,row_number() over (partition by DQ_APP_NAME,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_CHECK_ID,DQ_BATCH_ID) as rnk from %s  where dq_batch_id=%d  " %(dq_reslt_detl_master_stg,batch_id))
         dfsql_sel_df_summ_master_drop_rnk=dfsql_sel_df_summ_master.filter(dfsql_sel_df_summ_master["rnk"] == 1).drop("rnk")
         dfsql_sel_df_summ_master_drop_rnk.write.mode("append").partitionBy('dq_batch_id').saveAsTable(dq_reslt_summ_master)
         #dfsql_sel_df_summ_master_drop_rnk.show()
		 #dfsql_sel_df_log_detl.write.mode("append").partitionBy('dq_batch_id').saveAsTable(dq_log_detl_tbl)