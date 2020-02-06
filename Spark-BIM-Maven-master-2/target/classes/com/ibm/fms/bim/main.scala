package com.ibm.fms.bim

import com.ibm.fms.bim.dbread.ConfigCycle
import org.apache.spark.sql.DataFrame
import com.ibm.fms.bim.Calculate.CalculateCLT

import scala.util.Sorting.quickSort

object main {


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

  def  main(args: Array[String]): Unit = {



    val configCycle = new ConfigCycle
    val calculate = new CalculateCLT
    val EFFC_DATE_SQL = configCycle.getEffcDate("ITT_CURRENT_CYCLE")
    val REQ_IMT_ID = 230

    calculate.calculateCLTREV(EFFC_DATE_SQL,REQ_IMT_ID).printSchema()

  }
}
