from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext

conf = (SparkConf()
  .setAppName("data_export")
  .set("spark.dynamicAllocation.enabled","true")
  .set("spark.shuffle.service.enabled","true"))
  
sc = SparkContext(conf = conf)
  
# Create a Hive Context
sqlContext = HiveContext(sc)
  
print "Reading Hive table..."
custDf = sqlContext.table("l4_airlines.cust_prod_detl")
#dept=sqlContext.table("iris_schema.prod_dept")

	
custDf.write.jdbc(url="jdbc:mysql://192.168.1.6:3306/sanjeeb"
                  "?user=sp57529&password=User1313$",
              table="cust_prod_detl",
              mode="overwrite",
              properties={"driver": 'com.mysql.jdbc.Driver'})
sc.stop()
