from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext ,SQLContext,Row,HiveContext
from pyspark.sql.functions import lit
from datetime import datetime
import datetime
import sys
from AllChkGen import genQuery

# Three arguements are required to pass.
# 1st Arguement: Application NM.
# 2nd Arguement: Source Schema NM.
# 3rd Arguement: Source Tbl NM.


if __name__ == "__main__":
    if len(sys.argv) < 5 :
        print("Input Parameter Missing ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName=" DQ "+sys.argv[3])
    sqlContext=HiveContext(sc)
    
    detail_sqls=[]
	
    # Command Line Arguement
    app_nm=sys.argv[1]
    src_schema=sys.argv[2]
    src_tbl=sys.argv[3]
    dq_schema=sys.argv[4]
	
    dq_app_config=dq_schema+'.'+'dq_app_config'
    # dq_chk_master=dq_schema+'.'+'dq_check_master'
    dq_chk_master=dq_schema+'.'+'DQ_ALL_CHK_CONFIG'
    print ("dq_schema : " ,dq_schema)
    print ("dq_app_config : " ,dq_app_config)
    print ("dq_chk_master : " ,dq_chk_master)
    # print ("dq_reslt_single_view : " ,dq_reslt_single_view)
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
	
    dfsql_sel_df=sqlContext.sql("select dq_src_schema,dq_src_tbl from %s " %(dq_app_config))
    dfsql_sel_filter=dfsql_sel_df.where(dfsql_sel_df['dq_src_tbl'] == src_tbl).rdd
    if dfsql_sel_filter.count() == 0:
        print(" ************* DQ_APP_CONFIG has no matching record for %s . Please check the input Parameter or DQ_APP_CONFIG for further analysis  **************" %(src_tbl), file=sys.stderr)
        exit(-1)
    else:
        # Converting into a RDD for iteration.Fetching source information such as tbl nm and schema nm. Cahcing the table .
        dfsql_sel_rdd_list=dfsql_sel_filter.map(list)
        for line in dfsql_sel_rdd_list.collect():
            schema_nm=line[0] 
            tbl_nm=line[1] 
            cach_tbl_nm=tbl_nm
            query="select * from %s.%s" %(schema_nm,tbl_nm)
            #dfsql_sel_df=sqlContext.sql(query )
            dfsql_sel_df = sqlContext.load(
			  source="jdbc",\
			  url="jdbc:sqlserver://IRIS-CSG-740\SQLEXPRESS;database=iris_schema;user=sa;password=password@1",\
			  driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",\
			  dbtable="cust_prod_detl")
			  
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
                        dfsql_chk_master=sqlContext.sql('''select '%s' DQ_APP_NAME,'PRC_DQ_CHECK' dq_prc_name,'%s' dq_batch_start_dt,DQ_CHECK_ID,DQ_SRC_SCHEMA,DQ_SRC_TBL,DQ_SRC_COL from %s where DQ_SRC_TBL='%s' '''%(app_nm,dq_batch_start_dt,dq_chk_master,tbl_nm) )
                        dfsql_chk_master_fl=dfsql_chk_master.where(dfsql_chk_master['DQ_SRC_TBL'] == src_tbl).drop('DQ_SRC_TBL')
                        dfsql_chk_master_fl.show()
                        print ("all good")
						
						
		print("going to get the contents from DQ_ALL_CHK_CONFIG table ***************************************")
		
		dfsql_chk_master_rdd_list=sqlContext.sql('''select * from %s where DQ_SRC_TBL='%s' '''%(dq_chk_master,tbl_nm)).map(list)
		print ("all good so far")
		dt1=datetime.datetime.now()
                dq_exec_start_tm=('%02d%02d%02d%02d%02d%02d%d'%(dt1.year,dt1.month,dt1.day,dt1.hour,dt1.minute,dt1.second,dt1.microsecond))[:-4]
		for line1 in dfsql_chk_master_rdd_list.collect():
			print ("all very very  good")
			# each row fed one by one to python code to generate the query
			# each query then executed
			Name = line1[0] 
			SrcName = line1[1] 
			ChkId = line1[2]  
			Desp =  line1[3] 
			SrcTab = line1[4] 
			SrcCol = line1[5] 
			PK1 = line1[6] 
			PK2 = line1[7] 
			PK3 = line1[8] 
			PK4 = line1[9] 
			PK5 = line1[10] 
			NullChk = line1[11] 
			FtrRule = line1[12] 
			NullChkThrePer = line1[13] 
			LenChk = line1[14] 
			LenFtrRule = line1[15] 
			MinLen = line1[16] 
			MaxLen = line1[17] 
			LenChkThrePer = line1[18] 
			LovChk = line1[19] 
			LovFtrRule = line1[20] 
			LovChkThrePer = line1[21] 
			RefChk = line1[22] 
			RefFtrRule = line1[23] 
			LkpCustSQL= line1[24] 
			LkpTblSchema = line1[25] 
			LkpTblNm = line1[26] 
			LkpTblKeyCustSQL = line1[27] 
			Cust_Sql = line1[28] 
			CustSQLTblNm = line1[29] 
			CustSQLTblNmCustSqlKey= line1[30] 
			RefChkThrePer = line1[31] 
			DupChk = line1[32] 
			DupFtrRule = line1[33] 
			DupChkThrePer= line1[34] 
			DataChk = line1[35] 
			DataFtrRule = line1[36] 
			DataChkThrePer = line1[37] 
			ChkCreatedDate = line1[38] 
			pkNames=PK1+','+PK2+','+PK3+','+PK4+','+PK5
			init_err_cnt=0
			query=genQuery(line1)
			# output_result=sqlContext.sql(query)
			# print(query)
			detail_sqls.append(query)
					
    for query in detail_sqls:
	print(query)
	
    sqlContext.uncacheTable(cach_tbl_nm)
    sc.stop()
    print("Execution is completed ...")	
