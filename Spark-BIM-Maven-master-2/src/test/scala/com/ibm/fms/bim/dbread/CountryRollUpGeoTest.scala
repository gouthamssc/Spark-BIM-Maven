package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.dbread.CountryRollUpGeo
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class CountryRollUpGeoTest extends FunSuite with SparkSessionTestWrapper{
  test("Test to get CountryRollupGeo function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val countryRollUpGeo = new CountryRollUpGeo()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("CTRYNUM", StringType, nullableBoolean)
      ,StructField("CTRY_NAME", StringType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_NAME", StringType, nullableBoolean)
      ,StructField("SUBMARKET_CD", StringType, nullableBoolean)
      ,StructField("BUDGET_CTRYNUM", StringType, nullableBoolean)
      ,StructField("BUDGET_SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("BASE_IMT_ID", LongType, nullableBoolean)
      ,StructField("BASE_IMT_CD", StringType, nullableBoolean)
      ,StructField("BASE_IMT_NAME", StringType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("IMT_CD", StringType, nullableBoolean)
      ,StructField("IMT_NAME", StringType, nullableBoolean)
      ,StructField("IOT_ID", LongType, nullableBoolean)
      ,StructField("IOT_CD", StringType, nullableBoolean)
      ,StructField("IOT_NAME", StringType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = countryRollUpGeo.getCountryRollUpGeo()

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
