package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.dbread.ConfigCycle
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class ConfigCycleTest extends FunSuite with SparkSessionTestWrapper{
  test("Test to get ConfigCycle getCycleDate function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val configCycle = new ConfigCycle()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(
       StructField("EFFC_DATE",DateType, nullableBoolean)
      ,StructField("DLET_DATE",DateType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = configCycle.getCycleDate("ITT_CURRENT_CYCLE")

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  test("Test to get ConfigCycle getEffcDate function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val configCycle = new ConfigCycle()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
            StructField("EFFC_DATE_SQL",DateType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = configCycle.getEffcDate("ITT_CURRENT_CYCLE")

//    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame)
//    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
//    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
