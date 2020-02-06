package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.dbread.Covref
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class CovrefTest extends FunSuite with SparkSessionTestWrapper{
  test("Test to get Covref function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val covref = new Covref()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(
       StructField("CUSTOMER_SET_ID",LongType, nullableBoolean)
      ,StructField("ITT_COVREF_ID", LongType, nullableBoolean)
      ,StructField("EFFC_DATE", DateType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)

    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = testHelperFunctions.createSQLDate(EFFC_DATE_STRING)
    val actualDataFrame = covref.getCovref(EFFC_DATE_SQL)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
