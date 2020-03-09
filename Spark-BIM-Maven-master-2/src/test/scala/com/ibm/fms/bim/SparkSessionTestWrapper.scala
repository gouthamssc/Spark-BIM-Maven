package com.ibm.fms.bim

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test example")
      .getOrCreate()
    spark.conf.set("spark.driver.memory", "600m");
  }
}
