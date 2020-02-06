package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.dbread.BMDivPCT
import org.apache.spark.sql.types.{ DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class BMDivPCTTest extends FunSuite with SparkSessionTestWrapper{
  test("Test to get BMDIVPCT function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val bmDivPCT = new BMDivPCT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("BMDIV_ID",LongType, nullableBoolean)
      ,StructField("MODELING_LVL_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = bmDivPCT.getBMDivPCT()

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
