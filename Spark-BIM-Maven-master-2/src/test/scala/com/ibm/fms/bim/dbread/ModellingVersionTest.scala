package com.ibm.fms.bim.dbread
import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite
class ModellingVersionTest extends FunSuite with SparkSessionTestWrapper {
  test("check whether the  getModellingVersion returns the expected Schema")
  {
    val testHelperFunctions = new TestHelperFunctions()
    val modellingVersion = new ModellingVersion()
    val nullableBoolean=true;

    val expectedSchema = StructType(Seq(StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
    ))

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = modellingVersion.getModellingVersion(0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

}
