package com.ibm.fms.bim.dbconnections

import org.apache.spark.sql.DataFrame


class ReadTable{
  def readTableToDF(table_name_string : String, PartitionColumnName : String = null , lowerBound : Int = 0, upperBound : Int = 0, numPartitions : Int = 1):DataFrame = {
     
  val databaseRead = new DatabaseConnection
  val sparkSessionBuilder= new SparkSessionBuilder()
  val spark = sparkSessionBuilder.getOrCreateSparkSession()

    var df = spark.emptyDataFrame
    val devProperties = databaseRead.getConnectionProperties()
    val url = databaseRead.getDBUrl()
    val db_schema = databaseRead.getDBSchema()
    Class.forName("com.ibm.db2.jcc.DB2Driver")

    var table_name = db_schema+"."+table_name_string
    
    
    try {
      if(PartitionColumnName == null){
        df = spark.read.jdbc(url, table_name , devProperties).toDF()
      }
      else {
        df = spark.read.jdbc( url = url, table = table_name ,columnName = PartitionColumnName , lowerBound = lowerBound, upperBound = upperBound, numPartitions = numPartitions, connectionProperties = devProperties).toDF()
      }
    } catch {
      case ex: java.sql.SQLException =>
        println("Exception in FMSSPBC with Error:" + ex + ", Error Code:" + ex.getErrorCode())
        spark.stop()
    }
    df
  } 
}