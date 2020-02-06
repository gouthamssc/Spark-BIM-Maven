package com.ibm.fms.bim.dbread
import java.sql.Date

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite
class TargetAcctTest extends FunSuite with SparkSessionTestWrapper {
  test("check whether the  getTargetAccountID  returns the expected Schema") {
    val testHelperFunctions = new TestHelperFunctions()
    val targetAcct = new TargetAcct()
    val nullableBoolean = true;

    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("ITT_COVREF_ID",LongType, nullableBoolean)
      ,StructField("TARGET_ACCT_CD",StringType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
    )
    )

    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark, expectedSchema)
    val actualDataFrame = targetAcct.getTargetAccountID(EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}