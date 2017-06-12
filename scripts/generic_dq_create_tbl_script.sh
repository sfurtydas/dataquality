#!/bin/sh
################################################################################
# Program      : create_db_script.sh
# Date Created : 10/06/2015
# Description  :
# Parameters   :  <ENV NAME>
#
# Modification history:
#
# Date         Author               Description
# ===========  ===================  ============================================
# 04/07/2016   Sanjeeb Panda                Creation
################################################################################

##############################################################
# INITIALIZE JOB VARIABLES
##############################################################

JOB_START_TIME=$(date "+%Y%m%d%H%M%S")
typeset Job_Name=$(basename $0)
typeset Dir_Name=$(dirname $0)
typeset Start_Time=$(date)

##############################################################
# TEST NUMBER OF PARAMETERS & ODATE TYPE
##############################################################
if [[ $# -ne 1 ]]; then
  echo "Invalid number of parameters. Please pass <ENV NAME>" > /dev/null
 
  exit 1
fi

env=$1
echo "Procesing enviorment is  :" $env > /dev/null

if [ "$env" != "home" ] ; then
  
  echo "Please provide required environment var. (home/taco/sit/uat/prod)" 
  
  exit 1
fi
 


##############################################################
# INVOKE PROJECT SPECIFIC  PARAMETERS
##############################################################

Dir_Name=${PWD}
export PROJECT_DIR=$(dirname ${Dir_Name})
export LOG_LOC=${PROJECT_DIR}/log
export SCRIPT_LOC=${PROJECT_DIR}/scripts
export HQL_LOC=${PROJECT_DIR}/hive
export HQL_TBL_LOC=${PROJECT_DIR}/hive/tables
export CONFIG_LOC=${PROJECT_DIR}/config
export SPARK_QUERY_LOC=${PROJECT_DIR}/spark
export LOGFILE=${LOG_LOC}/${Job_Name%%.*}_${env}_${JOB_START_TIME}.log
export CONFIG_FILE=${CONFIG_LOC}/${env}_config.txt
echo "\n START ${Job_Name}:  ${Start_Time}.\n" >> ${LOGFILE}



 
cfgfile=${CONFIG_LOC}/${env}_config.txt
 
url=""
 
if [ "$1" == "dev" ]; then
  echo "dev" ${LOGFILE}
  url="jdbc:hive2://bdgtmaster02i1d.nam.nsroot.net:10000/amlmkt_dq;principal=hive/bdgtmaster02i1d.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET;user pass"
elif [ "$1" == "uat" ]; then
  echo "uat" ${LOGFILE}
  url="jdbc:hive2://bdgtmaster02i1u.nam.nsroot.net:10000/amlmkt_dq;principal=hive/bdgtmaster02i1u.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET;user pass"
  
elif [ "$1" == "home" ]; then
  echo "home" ${LOGFILE}
  url="jdbc:hive2://localhost:10000/amlmkt_dq;user pass"
elif [ "$1" == "prod" ]; then
  echo "prod" ${LOGFILE}
  url="jdbc:hive2://bdgtmaster02i1p.nam.nsroot.net:10000/amlmkt_dq;principal=hive/bdgtmaster02i1p.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET;user pass"
else
  echo "taco" ${LOGFILE}
  url="jdbc:hive2://bdgtmaster12h1l.nam.nsroot.net:10000/default;principal=hive/bdgtmaster12h1l.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET;user pass"
 
fi
 
 
for hqlfile in ${HQL_TBL_LOC}/*.hql
do
                echo $hqlfile >> ${LOGFILE}
                echo $cfgfile >> ${LOGFILE}
                echo "beeline -u ${url} --silent -i $cfgfile -f $hqlfile" >> ${LOGFILE}
                      beeline -u ${url}  -i $cfgfile -f $hqlfile >> ${LOGFILE}
	
	if [ $? -eq 0 ]; then
			
			echo "Table creation is Completed for " $hqlfile  >> ${LOGFILE}
	else
		    echo "Table creation is Failed for " $hqlfile  >> ${LOGFILE}
				
			exit 1 
	fi
				
               
done
 
echo " **********   Script completed sucessfully *********" >> ${LOGFILE}

 