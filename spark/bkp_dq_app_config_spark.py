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
    if len(sys.argv) < 5 :
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Aml Markets DQ "+sys.argv[3])
    sqlContext=HiveContext(sc)
    
    # Command Line Arguement
    app_nm=sys.argv[1]
    src_schema=sys.argv[2]
    src_tbl=sys.argv[3]
    dq_schema=sys.argv[4]
    #restart_flag=sys.argv[5]
    if (len(sys.argv) == 6):
        restart_flag=sys.argv[5]
    else:
        restart_flag='N'
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
    print("src tbl",src_tbl)
    
    print ("Seting Hive Parameter  " )
           
    sqlContext.setConf("hive.exec.dynamic.partition ","true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode","true")
    sqlContext.setConf("hive.execution.engine","spark")
    sqlContext.setConf("hive.vectorized.execution.enabled","true")
    sqlContext.setConf("hive.vectorized.execution.reduce.enabled","true")
    
    dt=datetime.datetime.now()
    dq_batch_start_id=app_nm +'_'+('%02d%02d%02d%02d%02d%02d%d'%(dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second,dt.microsecond))[:-4]
    dq_batch_start_dt=dt.strftime('%Y%m%d')
    print ("dq_batch_start_id:",dq_batch_start_id)
    print ("dq_batch_start_dt:",dq_batch_start_dt)
    
    dfsql_sel_nxt_batch_id=sqlContext.sql("select nvl(max(dq_batch_id),1) batch_id from %s where dq_app_name='%s' and dq_src_tbl='BATCH_EXE_STATS'" %(dq_log_detl_tbl_stg,app_nm)).rdd
    dfsql_sel_nxt_batch_id_rdd_list=dfsql_sel_nxt_batch_id.map(list)
    for line in dfsql_sel_nxt_batch_id_rdd_list.collect():
        print ("all very very  good")
        batch_id=line[0]
        print ("batch_id : " ,batch_id)
        
    # Selecting & Filtering the required configuration details from dq_app_config table.
    # If there is no matching record present as per the argument passed , processing will be stopped.

    dfsql_sel_df=sqlContext.sql("select dq_src_schema,dq_src_tbl from %s " %(dq_app_config))
    dfsql_sel_filter=dfsql_sel_df.where(dfsql_sel_df['dq_src_tbl'] == src_tbl).rdd
    if dfsql_sel_filter.count() == 0:
        print(" ************* DQ_APP_CONFIG has no matching record for %s . Please check the input Parameter or DQ_APP_CONFIG for further analysis  **************" %(src_tbl), file=sys.stderr)
        exit(-1)
    else:
        # Converting into a RDD for iteration.Fetching source information such as tbl nm and schema nm. Cahcing the table .
        dfsql_sel_rdd_list=dfsql_sel_filter.map(list)
        for line in dfsql_sel_rdd_list.collect():
            schema_nm=line[0].encode('utf-8')
            tbl_nm=line[1].encode('utf-8')
            cach_tbl_nm=tbl_nm
            query="select * from %s.%s" %(schema_nm,tbl_nm)
            dfsql_sel_df=sqlContext.sql(query )
            tot_rec_cnt_src_tbl=dfsql_sel_df.count()
            # Cheking source table record count. If the record count is throw error..
            if tot_rec_cnt_src_tbl == 0:
                print(" ***************** %s.%s has 0 record for DQ processing. Please check source table for further processing. ***************" %(schema_nm,tbl_nm),file=sys.stderr)
                exit(-1)
            else:
                dfsql_sel_df.registerTempTable(cach_tbl_nm)
                sqlContext.cacheTable(cach_tbl_nm)
                cach_tbl_cnt=sqlContext.sql('''select count(1) as cnt from %s ''' %(cach_tbl_nm)).rdd
                cach_tbl_cnt_list=cach_tbl_cnt.map(list)
                for line2 in cach_tbl_cnt_list.collect():
                    cach_tbl_cnt1=int(line2[0])
                    # Cheking caching is done properly or not by comparing source tbl cnt and cahc tbl cnt..
                    print ("cach_tbl_cnt :" ,cach_tbl_cnt1)
                    print ("tot_rec_cnt_src_tbl :" , tot_rec_cnt_src_tbl)
                    if  ((cach_tbl_cnt1) !=(tot_rec_cnt_src_tbl)) :
                        print(" ***************  %s couln't be cached properly. *****************" %(cach_tbl_nm), file=sys.stderr)
                        exit(-1)
                    else:
                        dfsql_chk_master=sqlContext.sql('''select '%s' DQ_APP_NAME,'PRC_DQ_CHECK' dq_prc_name,'%s' dq_batch_start_dt,DQ_CHECK_ID,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL,DQ_CHK_TYPE,DQ_DETL_HQL,DQ_THRESHOLD_PER from %s where DQ_SRC_TBL='%s' '''%(app_nm,dq_batch_start_dt,dq_chk_master,tbl_nm) )
                        dfsql_chk_master_fl=dfsql_chk_master.where(dfsql_chk_master['DQ_SRC_TBL'] == src_tbl).drop('DQ_SRC_TBL')
                        dfsql_chk_master_fl.show()
                        print ("all good")
                    if (restart_flag=='Y') :
                        dfsql_log_detl_stg_restart=sqlContext.sql(''' select DQ_CHECK_ID PROCESSED_CHECK_ID,DQ_RUN_STATUS from %s where dq_batch_id=%d and DQ_RUN_STATUS='COMPLETED' and DQ_SRC_TBL='%s'  ''' %(dq_log_detl_tbl_stg,batch_id,src_tbl))
                        dfsql_log_detl_stg_restart_processed = dfsql_chk_master.join(dfsql_log_detl_stg_restart,(dfsql_chk_master_fl.DQ_CHECK_ID==dfsql_log_detl_stg_restart.PROCESSED_CHECK_ID),'left_outer')
                        dfsql_log_detl_stg_restart_processed.show()
                        dfsql_log_detl_stg_restart_fl=dfsql_log_detl_stg_restart_processed.where(dfsql_log_detl_stg_restart_processed['PROCESSED_CHECK_ID'].isNull() )
                        dfsql_log_detl_stg_restart_fl.show()
                        dfsql_chk_master_rdd=dfsql_log_detl_stg_restart_fl.rdd
                    else:
                        print(" there will full restart")					
                        # Converting into a RDD for iteration.For each check configured in DQ check master the loop will process those many times .
                        dfsql_chk_master_rdd=dfsql_chk_master.rdd
                    dfsql_chk_master_rdd_list=dfsql_chk_master_rdd.map(list)
                    print ("all good so far")
                    for line1 in dfsql_chk_master_rdd_list.collect():
                        print ("all very very  good")
                        init_err_cnt=0
                        app_nm=line1[0].encode('utf-8')
                        print ("DQ_APP_NAME :" ,(app_nm))
                        chk_id=line1[3].encode('utf-8')
                        print ("DQ_CHECK_ID :" ,(chk_id))
                        schema_nm=line1[4].encode('utf-8')
                        print ("DQ_SRC_SCHEMA :" ,(schema_nm))
                        col_nm=line1[6].encode('utf-8')
                        print ("DQ_SRC_COL :" ,(col_nm))
                        chk_type=line1[7].encode('utf-8')
                        print ("DQ_CHK_TYPE :" ,(chk_type))
                        query=line1[8].encode('utf-8')
                        print ("DQ_DETL_HQL :" ,query)
                        dq_threshold_per=line1[9]
                        print ("DQ_THRESHOLD_PER :" ,dq_threshold_per)
                        dt1=datetime.datetime.now()
                        dq_exec_start_tm=('%02d%02d%02d%02d%02d%02d%d'%(dt1.year,dt1.month,dt1.day,dt1.hour,dt1.minute,dt1.second,dt1.microsecond))[:-4]
                        dfsql_log_detl_filter=dfsql_chk_master_fl.where(dfsql_chk_master['DQ_CHECK_ID'] == chk_id)
                        dfsql_log_detl_insert_start=dfsql_log_detl_filter.withColumn("DQ_TOT_REC_CNT",lit(tot_rec_cnt_src_tbl)).withColumn("DQ_ERR_REC_CNT",lit(init_err_cnt)).withColumn("DQ_RUN_STATUS",lit("STARTED")).withColumn("DQ_RUN_TM",lit(dq_exec_start_tm)).withColumn("dq_src_tbl",lit(src_tbl)).withColumn("dq_batch_id",lit(batch_id))
                        dfsql_log_detl_insert_start.show()
                        # Inserting into dq_log_detl_tbl_stg  for check started..
                        dfsql_log_detl_insert_start.write.mode("append").partitionBy('dq_src_tbl','dq_batch_id').saveAsTable(dq_log_detl_tbl_stg)
                        # Calculating error record by using the check master detl hwl query..
                        dfsql_reslt_detl_master_insert=sqlContext.sql(query)
                        err_rec_cnt=dfsql_reslt_detl_master_insert.count()
                        dq_err_per=(err_rec_cnt/tot_rec_cnt_src_tbl)*100
                        print ("dq_err_per :",str(dq_err_per))
                        dq_thrhold_flag_fun= lambda x,y : 1 if  x > y  else 0
                        dq_threshold_flag=dq_thrhold_flag_fun(dq_err_per,dq_threshold_per)
                        print ("dq_thrhold_flag",dq_threshold_flag)
                        dfsql_reslt_detl_master_insert1=dfsql_reslt_detl_master_insert.withColumn("DQ_TOT_REC_CNT",lit(tot_rec_cnt_src_tbl)).withColumn("DQ_ERR_REC_CNT",lit(err_rec_cnt)).withColumn("DQ_THRHOLD_FLAG",lit(dq_threshold_flag)).withColumn("DQ_RUN_DT",lit(dq_batch_start_dt)).withColumn("DQ_APP_NAME",lit(app_nm)).withColumn("DQ_SRC_SCHEMA",lit(schema_nm)).withColumn("DQ_SRC_COL",lit(col_nm)).withColumn("DQ_CHK_TYPE",lit(chk_type)).withColumn("DQ_RUN_TM",lit(dq_exec_start_tm)).withColumn("dq_src_tbl",lit(tbl_nm)).withColumn("dq_batch_id",lit(batch_id))
                        dfsql_reslt_detl_master_insert1.show()
                        # dq_reslt_detl_master_stg is inserted with error record ..
                        dfsql_reslt_detl_master_insert1.write.mode("append").partitionBy('dq_src_tbl','dq_batch_id').saveAsTable(dq_reslt_detl_master_stg)
                        dt2=datetime.datetime.now()
                        dq_exec_end_tm=('%02d%02d%02d%02d%02d%02d%d'%(dt2.year,dt2.month,dt2.day,dt2.hour,dt2.minute,dt2.second,dt2.microsecond))[:-4]
                        dfsql_log_detl_insert_end=dfsql_log_detl_filter.withColumn("DQ_TOT_REC_CNT",lit(tot_rec_cnt_src_tbl)).withColumn("DQ_ERR_REC_CNT",lit(err_rec_cnt)).withColumn("DQ_RUN_STATUS",lit("COMPLETED")).withColumn("DQ_RUN_TM",lit(dq_exec_end_tm)).withColumn("dq_src_tbl",lit(src_tbl)).withColumn("dq_batch_id",lit(batch_id))
                        # Inserting into dq_log_detl_tbl_stg  for check completed ..
                        dfsql_log_detl_insert_end.write.mode("append").partitionBy('dq_src_tbl','dq_batch_id').saveAsTable(dq_log_detl_tbl_stg)
    sqlContext.uncacheTable(cach_tbl_nm)
    sc.stop()
    print("Execution is completed ...")
