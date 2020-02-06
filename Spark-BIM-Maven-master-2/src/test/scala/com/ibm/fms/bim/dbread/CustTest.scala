package com.ibm.fms.bim.dbread
import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class CustTest  extends FunSuite with SparkSessionTestWrapper{
  test("check whether the  getValidCust returns the expected Schema")
  {
    val testHelperFunctions = new TestHelperFunctions()
    val cust = new Cust()
    val nullableBoolean=true;

    val expectedSchema = StructType(Seq(StructField("ITT_CUST_ID", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("SAP_CUST_NBR",StringType, nullableBoolean)
      ,StructField("CI_CUST_NBR",StringType, nullableBoolean)
      ,StructField("CUSTNUM",StringType, nullableBoolean)
      ,StructField("CTRYNUM",StringType, nullableBoolean)
      ,StructField("CUST_NAME",StringType, nullableBoolean)
      ,StructField("COV_TYPE",StringType, nullableBoolean)
      ,StructField("COV_ID",StringType, nullableBoolean)
      ,StructField("COV_NAME",StringType, nullableBoolean)
    ))

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = cust.getValidCust(0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}