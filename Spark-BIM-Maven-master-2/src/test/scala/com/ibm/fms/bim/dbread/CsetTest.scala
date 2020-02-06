package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.dbread.CSET
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class CsetTest extends FunSuite with SparkSessionTestWrapper{
  test("Test to get CSET function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val cset = new CSET()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(
       StructField("CUST_SET_NAME", StringType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("CUSTOMER_SET_ID", LongType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = cset.getCSET()

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
