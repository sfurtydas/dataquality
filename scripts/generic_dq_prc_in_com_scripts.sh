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
if [[ $# -ne 4 ]]; then
  echo "Invalid number of parameters. Please pass <ENV NAME> <APPLICATION NM> <SOURCE SCHEMA NM> <SOURCE TBL NM>" > /dev/null
 
  exit 1
fi


# 1st Arguement: ENV NM.
# 2nd Arguement: APPLICATION NM.
# 3rd Arguement: SOURCE SCHEMA NM.
# 4th Arguement: SOURCE TBL NM.

env=$1
app_nm=$2
prc_nm=$3
status=$4

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
export SPARK_SCRIPTS=${PROJECT_DIR}/spark
export LOGFILE=${LOG_LOC}/${Job_Name%%.*}_${env}_${JOB_START_TIME}.log
export CONFIG_FILE=${CONFIG_LOC}/${env}_config.txt
echo "\n START ${Job_Name}:  ${Start_Time}.\n" >> ${LOGFILE}

cfgfile=${CONFIG_LOC}/${env}_config.txt
echo "cfgfile :" $cfgfile >> ${LOGFILE}

export dq_db_nm=`grep dq_schema $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export hdfs_path=`grep hdfs_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_db_path=`grep dq_db_path $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`
export dq_schema=`grep dq_schema $cfgfile | awk '{ print $2}' |awk -F "=" '{ print $2}' | awk -F ";" '{print $1}'`

echo "dq_database_nm :" $dq_db_nm >> ${LOGFILE}
echo "hdfs_path :" $hdfs_path >> ${LOGFILE}
echo "dq_database_path :" $dq_db_path>> ${LOGFILE}
echo "dq_schema :" $dq_schema>> ${LOGFILE}


case "$env" in
                sit)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdgtmaster02i1d.nam.nsroot.net:10000/default;principal=hive/bdgtmaster02i1d.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 --silent -e "drop database if exists ${dq_db_nm} cascade;"
                beeline -u $hiveserver2 --silent -e "create database ${dq_db_nm} location '/user/sp57529/data/${dq_db_nm}';"
                ;;  
                home)
                echo $env >> ${LOGFILE}
				echo "spark-submit ${SPARK_SCRIPTS}/dq_app_config_spark.py  $app_nm $src_schema_nm $src_tbl_nm $dq_schema"
                spark-submit ${SPARK_SCRIPTS}/dq_prc_ini_com_spark.py  $dq_schema $prc_nm $app_nm $status
                ;; 				
                uat)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdgtmaster02i1u.nam.nsroot.net:10000/default;principal=hive/bdgtmaster02i1u.nam.nsroot.net@APACUXUAT.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 -e "drop database if exists amlmkt_dq cascade;"
                beeline -u $hiveserver2 -e "create database amlmkt_dq location '/user/sp57529/data/amlmkt_dq';"
                ;;
                prod)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdsgmaster02i1p.nam.nsroot.net:10000/default;principal=hive/bdsgmaster02i1p.nam.nsroot.net@APACUXPRD.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 -e "drop database if exists amlmkt_dq cascade;"
                beeline -u $hiveserver2 -e "create database amlmkt_dq location '/user/sp57529/data/amlmkt_dq';"
                ;;
                taco)
                echo $env >> ${LOGFILE}
                hiveserver2='"jdbc:hive2://bdgtmaster12h1l.nam.nsroot.net:10000/default;principal=hive/bdgtmaster12h1l.nam.nsroot.net@NAMUXDEV.DYN.NSROOT.NET"'
                beeline -u $hiveserver2 -e "drop database if exists amlmkt_dq cascade;"
                beeline -u $hiveserver2 -e "create database amlmkt_dq location '/user/sp57529/data/amlmkt_dq';"
                ;;


esac

	if [ $? -eq 0 ]; then
			
			echo " ************************  Data Quality check execution is completed in  $env for  $app_nm $src_schema_nm $src_tbl_nm *********************************">> ${LOGFILE}
	else
		    echo "************************   Data Base creation is failed in  $env for  $app_nm $src_schema_nm $src_tbl_nm Please look into this.************************ ">> ${LOGFILE}
				exit 1 
	fi
				
 
