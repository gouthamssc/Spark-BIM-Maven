package com.ibm.fms.bim.dbread
import java.sql.Date

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class CustPCTTest  extends FunSuite with SparkSessionTestWrapper {
  test("check whether the  getCustPCT returns the expected Schema") {
    val testHelperFunctions = new TestHelperFunctions()
    val custPCT = new CustPCT()
    val nullableBoolean = true;
    val expectedSchema = StructType(Seq(StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      , StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      , StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      , StructField("IMT_ID", LongType, nullableBoolean)
      , StructField("ITT_CUST_ID", LongType, nullableBoolean)
      ,StructField("CUST_ACCT_PCT",  DataTypes.createDecimalType(11,10), nullableBoolean)
    )
    )
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark, expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)
    val actualDataFrame =custPCT.getCustPCT(EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}