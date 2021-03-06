from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
from pyspark.sql.functions import lit
from datetime import datetime
import datetime
import sys
import time

# TWO arguements are required to pass.
# 1st Arguement: DQ_SCHEMA_NM.
# 2nd Arguement: DQ_PRC_NM.
# 3rd Arguement: APP_NM.
# 4th Arguement: STATUS.



if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Aml Markets DQ")
    sqlContext=HiveContext(sc)
    # Command Line Arguement
    dq_schema=sys.argv[1]
    prc_nm=sys.argv[2]
    dq_app_nm=sys.argv[3]
    status=sys.argv[4]
    job_status1= ['STARTED' if status=='INI' else 'COMPLETED' if status=='COM' else 'NONE']
    job_status=job_status1[0]
    if (job_status =='NONE'):
        print("Input Parameter has wrong values. It should be ini or com ", file=sys.stderr)
        exit(-1)
    else:
        
        dt1=datetime.datetime.now()
        dq_exec_start_tm=('%02d%02d%02d%02d%02d%02d%d'%(dt1.year,dt1.month,dt1.day,dt1.hour,dt1.minute,dt1.second,dt1.microsecond))[:-4]
        #dq_batch_start_id=app_nm +'_'+('%02d%02d%02d%02d%02d%02d%d'%(dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second,dt.microsecond))[:-4]
        dq_batch_start_dt=dt1.strftime('%Y%m%d')
        # DQ table details
        #dq_schema='amlmkt_dq'
        dq_app_config=dq_schema+'.'+'dq_app_config'
        dq_chk_master=dq_schema+'.'+'dq_check_master'
        dq_log_detl_tbl_stg=dq_schema+'.'+'dq_log_detl_stg'
        dq_log_detl_tbl=dq_schema+'.'+'dq_log_detl'
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

        print ("Seting Hive Parameter  " )

        sqlContext.setConf("hive.exec.dynamic.partition ","true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

        #print ("dq_batch_start_id:",dq_batch_start_id)
        print ("dq_batch_start_dt:",dq_batch_start_dt)
        # Selecting & Filtering the required configuration details from dq_app_config table.
        # If there is no matching record present as per the argument passed , processing will be stopped.
        dfsql_sel_nxt_batch_id_ini=sqlContext.sql("select nvl(max(dq_batch_id),0)+1 batch_id from %s where  dq_src_tbl='BATCH_EXE_STATS'" %(dq_log_detl_tbl_stg)).rdd
        dfsql_sel_nxt_batch_id_com=sqlContext.sql("select nvl(max(dq_batch_id),0) batch_id from %s where dq_app_name='%s' and  dq_src_tbl='BATCH_EXE_STATS'" %(dq_log_detl_tbl_stg,dq_app_nm)).rdd
        dfsql_sel_nxt_batch_id_rdd_list=dfsql_sel_nxt_batch_id_ini.map(list)
        dfsql_sel_nxt_batch_id_com_list=dfsql_sel_nxt_batch_id_com.map(list)
        if (status=='INI'):
            for line in dfsql_sel_nxt_batch_id_rdd_list.collect():
                print ("all very very  good")
                batch_id=line[0]
                print ("batch_id_ini : " ,batch_id)
                dfsql_app_config=sqlContext.sql("select '%s' dq_app_nm,'%s' dq_prc_name, '%s' dq_batch_start_dt,'DQ_CHK_0000' dq_check_id,'DQ_SRC_SCHEMA' dq_src_schema, 'DQ_SRC_COL' dq_src_col,'DQ_CHK' dq_chk_type ,'DQ_DETL_HQL' dq_detl_hql, 0 dq_threshold_per,0 dq_tot_rec_cnt,0 dq_err_rec_cnt,'%s' dq_run_status ,'BATCH_EXE_STATS' dq_src_tbl from %s limit 1" %(dq_app_nm,prc_nm,dq_batch_start_dt,job_status,dq_app_config))
                dfsql_app_config.show()
                dfsql_log_detl_stg=dfsql_app_config.withColumn("dq_run_tm",lit(dq_exec_start_tm)).withColumn("dq_batch_id",lit(batch_id))
                dfsql_log_detl_stg.show()
                dfsql_log_detl_stg.write.mode("append").partitionBy('dq_src_tbl','dq_batch_id').saveAsTable(dq_log_detl_tbl_stg)
        else:
            for line in dfsql_sel_nxt_batch_id_com_list.collect():
                print ("all very very  good")
                batch_id=line[0]
                print ("batch_id_com : " ,batch_id)
                dfsql_app_config=sqlContext.sql("select '%s' dq_app_nm,'%s' dq_prc_name, '%s' dq_batch_start_dt,'DQ_CHK_0000' dq_check_id,'DQ_SRC_SCHEMA' dq_src_schema, 'DQ_SRC_COL' dq_src_col,'DQ_CHK' dq_chk_type ,'DQ_DETL_HQL' dq_detl_hql, 0 dq_threshold_per,0 dq_tot_rec_cnt,0 dq_err_rec_cnt,'%s' dq_run_status ,'BATCH_EXE_STATS' dq_src_tbl from %s limit 1" %(dq_app_nm,prc_nm,dq_batch_start_dt,job_status,dq_app_config))
                dfsql_app_config.show()
                dfsql_log_detl_stg=dfsql_app_config.withColumn("dq_run_tm",lit(dq_exec_start_tm)).withColumn("dq_batch_id",lit(batch_id))
                dfsql_log_detl_stg.show()
                dfsql_log_detl_stg.write.mode("append").partitionBy('dq_src_tbl','dq_batch_id').saveAsTable(dq_log_detl_tbl_stg)
                                                                                                            
                print (" ******* All entries in the dq_log_detl_tbl_stg is completed *******")
                print ("******** The Final Load process will start to load all the log information into dq_log_detl_tbl table .  ******")
                #time.sleep(120)
                print (" Spark Context will shut down")
                sc.stop()
                sc = SparkContext(appName="Aml Markets DQ")
                sqlContext=HiveContext(sc)
                print ("Seting Hive Parameter  " )
                sqlContext.setConf("hive.exec.dynamic.partition ","true")
                sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
                dfsql_sel_df_log_detl=sqlContext.sql(" select DQ_APP_NAME,DQ_PRC_NAME,DQ_BATCH_START_DT,DQ_CHECK_ID,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL,DQ_CHK_TYPE,DQ_DETL_HQL,DQ_THRESHOLD_PER,DQ_TOT_REC_CNT,DQ_ERR_REC_CNT,DQ_RUN_STATUS,DQ_RUN_TM, dq_batch_id  from %s where dq_batch_id=%d  " %(dq_log_detl_tbl_stg,batch_id))
                dfsql_sel_df_log_detl.show()
                dfsql_sel_df_log_detl_filter_cnt=dfsql_sel_df_log_detl.count()
                if dfsql_sel_df_log_detl_filter_cnt == 0:
                    print(" ***************** %s.%s has No error record to be loaded in dq_log_detl_tbl.. ***************" %(dq_schema,dq_log_detl_tbl),file=sys.stderr)
                    exit(-1)
                else:
                    print(" ***************** %s.%s has %d error record to be loaded in dq_log_detl_tbl.. ***************" %(dq_schema,dq_log_detl_tbl,dfsql_sel_df_log_detl_filter_cnt),file=sys.stderr)
                    dfsql_sel_df_log_detl.write.mode("append").partitionBy('dq_batch_id').saveAsTable(dq_log_detl_tbl)
                    dfsql_sel_df_log_detl.show()
                    print (" Spark Context will shut down")
        print ("Intiation process is completed")
        print ("Spark Context will shut down")
    sc.stop()
    print ("Job is completed successfully")
