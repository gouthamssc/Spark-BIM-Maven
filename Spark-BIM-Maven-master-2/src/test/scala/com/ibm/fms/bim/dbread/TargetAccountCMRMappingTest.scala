package com.ibm.fms.bim.dbread
import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite
class TargetAccountCMRMappingTest extends FunSuite with SparkSessionTestWrapper {
  test("check whether the getValidCMRMapping  returns the expected Schema") {
    val testHelperFunctions = new TestHelperFunctions()
    val targetAccountCMRMapping = new TargetAccountCMRMapping()
    val nullableBoolean = true;

    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean) ,
      StructField("ITT_CUST_ID", LongType, nullableBoolean)
    )
    )

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = targetAccountCMRMapping.getValidCMRMapping(0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}