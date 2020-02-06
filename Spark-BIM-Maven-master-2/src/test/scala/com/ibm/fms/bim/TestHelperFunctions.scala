package com.ibm.fms.bim

import java.sql.Date

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Sorting.quickSort

class TestHelperFunctions {

  def assertDataFrameColumnCountEquals(actualDataFrame: DataFrame, expectedDataFrame: DataFrame): Boolean = {
    val actualDataFrameColumnCount=actualDataFrame.columns.length
    val expectedDataFrameColumnCount=expectedDataFrame.columns.length
    var returnValue=false
    if(actualDataFrameColumnCount==expectedDataFrameColumnCount){
      returnValue=true
    }
    returnValue
  }

  def assertDataFrameColumnNameEquals(actualDataFrame: DataFrame, expectedDataFrame: DataFrame): Boolean = {
    val actualDataFrameColumn=actualDataFrame.columns
    val expectedDataFrameColumn=expectedDataFrame.columns
    var returnValue=false

    quickSort(actualDataFrameColumn)
    quickSort(expectedDataFrameColumn)

    if(actualDataFrameColumn.corresponds(expectedDataFrameColumn){_.equalsIgnoreCase(_)})
    {
      returnValue = true
    }
    returnValue
  }

  def assertDataFrameSchemaEquals(actualDataFrame: DataFrame, expectedDataFrame: DataFrame): Boolean ={
    val actualDataFrameColumn=actualDataFrame.columns
    val expectedDataFrameColumn=expectedDataFrame.columns
    var returnValue=false

    quickSort(actualDataFrameColumn)
    quickSort(expectedDataFrameColumn)

    val actualDataFrameColumnSorted = actualDataFrame.select(actualDataFrameColumn.head,actualDataFrameColumn.tail : _*)
    val expectedDataFrameColumnSorted = expectedDataFrame.select(expectedDataFrameColumn.head,expectedDataFrameColumn.tail : _*)

    if(actualDataFrameColumnSorted.schema.toString().equalsIgnoreCase(expectedDataFrameColumnSorted.schema.toString()))
    {
      returnValue = true
    }
    returnValue
  }

  def createEmptyDataFrameWithGivenSchema(spark: SparkSession, schema: StructType): DataFrame ={

    val df = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], schema)
    df
  }

  def createSQLDate(DateString: String): Date ={
    Date.valueOf(DateString)
  }


}
