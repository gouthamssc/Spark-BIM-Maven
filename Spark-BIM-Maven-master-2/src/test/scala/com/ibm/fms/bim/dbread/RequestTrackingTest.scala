package com.ibm.fms.bim.dbread
import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class RequestTrackingTest extends FunSuite with SparkSessionTestWrapper {
  test("check whether the  getRequestTrackingID returns the expected Schema") {
    val testHelperFunctions = new TestHelperFunctions()
    val requestTracking = new RequestTracking()
    val nullableBoolean = true;

    val expectedSchema = StructType(Seq(
       StructField("IMT_ID", LongType, nullableBoolean)
    ))

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = requestTracking.getRequestTrackingID("","","")

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}