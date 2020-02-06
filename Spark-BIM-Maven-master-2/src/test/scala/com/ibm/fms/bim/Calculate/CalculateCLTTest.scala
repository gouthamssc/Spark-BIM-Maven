package com.ibm.fms.bim.Calculate

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class CalculateCLTTest extends FunSuite with SparkSessionTestWrapper {
  test("Column count of get Attrib value function is returning as expected without cast parameter passed"){
   val testHelperFunctions = new TestHelperFunctions()
   val calculateCLT = new CalculateCLT()
   val nullableBoolean=true
   val notnullableBoolean = false

    val expectedSchema = StructType(Seq(StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
     ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
     ,StructField("ITT_CUST_ID", LongType, nullableBoolean)
     ,StructField("IMT_ID", LongType, nullableBoolean)
     ,StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
     ,StructField("ACCOUNT_LVL_TARGET_ID", LongType, nullableBoolean)
     ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
     ,StructField("TARGET_AMOUNT", DoubleType, nullableBoolean)
     ,StructField("GP_TARGET_AMOUNT", DoubleType, nullableBoolean)
     ,StructField("ADJ_TARGET_AMOUNT", DoubleType, nullableBoolean)
     ,StructField("ADJ_GP_TARGET_AMOUNT", DoubleType, nullableBoolean)
     ,StructField("CUST_SET_NAME", StringType, nullableBoolean)
     ,StructField("TARGET_ACCT_CD", StringType, nullableBoolean)
     ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
     ,StructField("GP_TARGET_SKEW_PCT",  DataTypes.createDecimalType(11,10), nullableBoolean)
     ,StructField("REASON_CD", StringType, nullableBoolean)
     ,StructField("EFFC_DATE", DateType, nullableBoolean)
     ,StructField("DLET_DATE", DateType, nullableBoolean)
     ,StructField("GT10_TARGET_AMOUNT",DoubleType, nullableBoolean)
     ,StructField("LT10_TARGET_AMOUNT",DoubleType, nullableBoolean)
     ,StructField("ADJ_CLOUD_TARGET_AMOUNT",DoubleType,nullableBoolean)
     ,StructField("SAP_CUST_NBR",StringType, nullableBoolean)
     ,StructField("CI_CUST_NBR",StringType, nullableBoolean)
     ,StructField("CUSTNUM",StringType, nullableBoolean)
     ,StructField("CTRYNUM",StringType, nullableBoolean)
     ,StructField("CUST_NAME",StringType, nullableBoolean)
     ,StructField("COV_TYPE",StringType, nullableBoolean)
     ,StructField("COV_ID",StringType, nullableBoolean)
     ,StructField("COV_NAME",StringType, nullableBoolean)
     ,StructField("BMDIV_ID", LongType, nullableBoolean)
     ,StructField("APPR_STATUS", StringType, notnullableBoolean)
     ,StructField("LAST_ACT_SYS_CD", StringType, notnullableBoolean)
     ,StructField("LAST_ACT_USER_ID", IntegerType, notnullableBoolean)
     ,StructField("CREATE_TIMESTAMP", TimestampType, notnullableBoolean)
     ,StructField("LAST_UPT_TIME", TimestampType, notnullableBoolean)
    ))

    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = testHelperFunctions.createSQLDate(EFFC_DATE_STRING)
    val REQ_IMT_ID = 0

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = calculateCLT.calculateCLTREV(EFFC_DATE_SQL,REQ_IMT_ID)

   assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
   assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
   assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}