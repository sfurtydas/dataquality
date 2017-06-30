def genPK(PK1,PK2,PK3,PK4,PK5,SrcTab,tabAlias=''):
	
    if len(tabAlias)>0:
        SrcTab=tabAlias
      
    pk1 =  '\'-\'' if len(PK1)==0 else SrcTab+'.'+PK1
    pk2 =  '\'-\'' if len(PK2)==0 else SrcTab+'.'+PK2
    pk3 =  '\'-\''  if len(PK3)==0 else SrcTab+'.'+PK3
    pk4 =  '\'-\''  if len(PK4)==0 else SrcTab+'.'+PK4
    pk5 =  '\'-\''  if len(PK5)==0 else SrcTab+'.'+PK5
   
    return pk1,pk2,pk3,pk4,pk5

    
def genPKnames(PK1,PK2,PK3,PK4,PK5,SrcTab):
    pknames=('\'-\'' if len(PK1)==0 else SrcTab+'.'+PK1)  + \
                ('' if len(PK2)==0 else  ', '+SrcTab+'.'+ PK2) + \
                ('' if len(PK3)==0 else  ', '+SrcTab+'.'+ PK3 ) + \
                ('' if len(PK4)==0 else  ', '+SrcTab+'.'+ PK4 ) + \
                ('' if len(PK5)==0 else  ', '+SrcTab+'.'+ PK5 )
    return pknames

def NullChkCase(NullChk, errcol, FtrRule):
    # defining NULL case statament
    if NullChk=='N':
        return -1
    else:
        fil_cond = '1=1' if len(FtrRule)==0 else FtrRule
        CaseStmt = "case ({errcol} is null or {errcol} = '') and {fil_cond} When True Then 1 Else 0 end"\
                    .format(errcol=errcol, fil_cond=fil_cond)
        return CaseStmt
        #return 'case ('+errcol+' is null and '+errcol+" = '') and "+fil_cond+" When True Then 1 Else 0 end"


def LenChkCase(LenChk, errcol, LenFtrRule, MinLen, MaxLen):
    # defining Length case statament
    if LenChk=='N':
        return -1
    else:
        fil_cond = '1=1' if len(LenFtrRule)==0 else LenFtrRule
        CaseStmt = "case (length({errcol}) < {MinLen} or length({errcol}) > {MaxLen}) and {fil_cond} When True Then 1 Else 0 end"\
                    .format(errcol=errcol, fil_cond=fil_cond, MinLen=MinLen, MaxLen=MaxLen)
        return CaseStmt  
        #return "case (length("+errcol+") < "+MinLen+" or length("+errcol+") > "+MaxLen+") and "+fil_cond+" When True Then 1 Else 0 end"
    

def LovChkCase(LovChk, errcol, LovFtrRule):
    # defining Love case statament
    if LovChk=='N' or len(LovFtrRule)==0:
        return -1
    else:
        CaseStmt = "case ({errcol} not in {LovFtrRule}) When True Then 1 Else 0 end"\
                    .format(errcol=errcol, LovFtrRule=LovFtrRule)
        return CaseStmt  
        #return "case "+errcol+" not in "+LovFtrRule+" When True Then 1 Else 0 end"


def DataChkCase(DataChk, errcol, DataFtrRule, table, SrcCol):
    # defining DataType case statament
    if DataChk=='N':
        return -1
    else:    
        CaseStmt = "case (Not ({table}.{SrcCol} is null or {table}.{SrcCol} = '') and ({table}.{SrcCol} like {DataFtrRule})) When True Then 1 Else 0 end"\
                    .format(errcol=errcol, table=table, SrcCol=SrcCol,  DataFtrRule=DataFtrRule)
        return CaseStmt 
	
def DupChkCase(DupChk, DupFtrRule, ChkId, PK1, PK2, PK3, PK4, PK5, SrcTab, SrcCol, errcol):
    # defining Duplicate case statament
    if DupChk=='N':
        return -1
    else:
        
        Dup_fil_cond = '1=1' if len(DupFtrRule)==0 else DupFtrRule
        PartitionByKey=('\'-\'' if len(PK1)==0 else PK1) + \
                ('' if len(PK2)==0 else   ',' + PK2)  + \
                ('' if len(PK3)==0 else   ',' + PK3 ) + \
                ('' if len(PK4)==0 else   ',' + PK4 ) + \
                ('' if len(PK5)==0 else   ',' + PK5 ) 
        
        pk1, pk2, pk3, pk4, pk5 = genPK(PK1,PK2,PK3,PK4,PK5,SrcTab)
        
        Dup_detail_query = "select {pk1}, {pk2}, {pk3}, {pk4},  1 CNT from {SrcTab} where {Dup_fil_cond} GROUP BY {PartitionByKey} HAVING COUNT(1)>1"\
                    .format(pk1=pk1, pk2=pk2, pk3=pk3, pk4=pk4, pk5=pk5, SrcTab=SrcTab,Dup_fil_cond=Dup_fil_cond,PartitionByKey=PartitionByKey );   
        return Dup_detail_query	
	
def genQuery(line1):
   
		
        Name = line1[0].encode('utf-8')
        SrcName = line1[1].encode('utf-8')
        ChkId = line1[2].encode('utf-8') 
        Desp =  line1[3].encode('utf-8')
        SrcTab = line1[4].encode('utf-8')
        SrcCol = line1[5].encode('utf-8')
        PK1 = line1[6].encode('utf-8')
        PK2 = line1[7].encode('utf-8')
        PK3 = line1[8].encode('utf-8')
        PK4 = line1[9].encode('utf-8')
        PK5 = line1[10].encode('utf-8')
        NullChk = line1[11].encode('utf-8')
        FtrRule = line1[12].encode('utf-8')
        NullChkThrePer = line1[13].encode('utf-8')
        LenChk = line1[14].encode('utf-8')
        LenFtrRule = line1[15].encode('utf-8')
        MinLen = line1[16].encode('utf-8')
        MaxLen = line1[17].encode('utf-8')
        LenChkThrePer = line1[18].encode('utf-8')
        LovChk = line1[19].encode('utf-8')
        LovFtrRule = line1[20].encode('utf-8')
        LovChkThrePer = line1[21].encode('utf-8')
        RefChk = line1[22].encode('utf-8')
        RefFtrRule = line1[23].encode('utf-8')
        LkpCustSQL= line1[24].encode('utf-8')
        LkpTblSchema = line1[25].encode('utf-8')
        LkpTblNm = line1[26].encode('utf-8')
        LkpTblKeyCustSQL = line1[27].encode('utf-8')
        Cust_Sql = line1[28].encode('utf-8')
        CustSQLTblNm = line1[29].encode('utf-8')
        CustSQLTblNmCustSqlKey= line1[30].encode('utf-8')
        RefChkThrePer = line1[31].encode('utf-8')
        DupChk = line1[32].encode('utf-8')
        DupFtrRule = line1[33].encode('utf-8')
        DupChkThrePer= line1[34].encode('utf-8')
        DataChk = line1[35].encode('utf-8')
        DataFtrRule = line1[36].encode('utf-8')
        DataChkThrePer = line1[37].encode('utf-8')
        ChkCreatedDate = line1[38]
    
        
    
        if DupChk=='N':
            pknames = genPKnames(PK1,PK2,PK3,PK4,PK5,SrcTab)
            pk1, pk2, pk3, pk4, pk5 = genPK(PK1,PK2,PK3,PK4,PK5,SrcTab)
            errcol = SrcTab+'.'+SrcCol

        
            detail_query = "select '{chk_id}', '{pknames}' pknames, {pk1} pk1, {pk2} pk2, {pk3} pk3, {pk4} pk4, {pk5} pk5, {errcol} errcol, \
			{NullChkStmt} NullChkResult, {LenChkStmt} LenChkResult, {LovChkStmt} LovChkResult, {DataChkStmt} DataChkResult, -1  DupChkResult from {SrcTab} \
			where ({errcol} is not null and {errcol} != '')"   \
                    .format(chk_id=ChkId, pknames=pknames, pk1=pk1, pk2=pk2, pk3=pk3, pk4=pk4, pk5=pk5, errcol=errcol, SrcTab=SrcTab,   \
                    	NullChkStmt=NullChkCase(NullChk, errcol, FtrRule), LenChkStmt=LenChkCase(LenChk, errcol, LenFtrRule, MinLen, MaxLen), LovChkStmt=LovChkCase(LovChk, errcol, LovFtrRule), DataChkStmt=DataChkCase(DataChk, errcol, DataFtrRule, SrcTab, SrcCol) );
              
        else:
            pknames = genPKnames(PK1,PK2,PK3,PK4,PK5,SrcTab)
            pk1, pk2, pk3, pk4, pk5= genPK(PK1,PK2,PK3,PK4,PK5,SrcTab,'A')
            errcol = 'A.'+SrcCol
            pk11, pk21, pk31, pk41, pk51= genPK(PK1,PK2,PK3,PK4,PK5,SrcTab,'B')
            
            dup_table = DupChkCase(DupChk, DupFtrRule, ChkId, PK1, PK2, PK3, PK4, PK5, SrcTab, SrcCol, errcol)
            detail_query = "select '{chk_id}', '{pknames}' pknames, {pk1} pk1, {pk2} pk2, {pk3} pk3, {pk4} pk4, {pk5} pk5, {errcol} errcol, \
			{NullChkStmt} NullChkResult, {LenChkStmt} LenChkResult, {LovChkStmt} LovChkResult, {DataChkStmt} DataChkResult, B.CNT DupChkResult  from {SrcTab} A left join ({dup_table}) B\
			on {pk1}={pk11} and {pk2}={pk21} and {pk3}={pk31} and {pk4}={pk41} and {pk5}={pk51} \
			where ({errcol} is not null and {errcol} != '')"   \
                    .format(chk_id=ChkId, pknames=pknames, pk1=pk1, pk2=pk2, pk3=pk3, pk4=pk4, pk5=pk5, errcol=errcol, SrcTab=SrcTab,   \
                            pk11=pk11, pk21=pk21, pk31=pk31, pk41=pk41, pk51=pk51, \
                            NullChkStmt=NullChkCase(NullChk, errcol, FtrRule), LenChkStmt=LenChkCase(LenChk, errcol, LenFtrRule, MinLen, MaxLen), \
                            LovChkStmt=LovChkCase(LovChk, errcol, LovFtrRule), DataChkStmt=DataChkCase(DataChk, errcol, DataFtrRule, 'A', SrcCol),dup_table=dup_table );
       
        
	return detail_query
        
  
