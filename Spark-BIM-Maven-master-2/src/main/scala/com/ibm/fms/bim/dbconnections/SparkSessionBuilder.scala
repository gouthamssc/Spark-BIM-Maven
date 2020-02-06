package com.ibm.fms.bim.dbconnections

import org.apache.spark.sql.SparkSession

class SparkSessionBuilder extends SparkSessionWrapper {
  def getOrCreateSparkSession(): SparkSession ={
    spark
  }
}
