package com.ibm.fms.bim.Joins

import java.sql.Date

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.Joins.CLT
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class CLTTest extends FunSuite with SparkSessionTestWrapper{

  //1.joinCountryRollUpGeoWithParticipatingMarketTogetRegisteredSubMarket
  test("Test to get joinCountryRollUpGeoWithParticipatingMarketTogetRegisteredSubMarket function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val CLTTest = new CLT()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(
      StructField("IOT_ID", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("IMT_NAME", StringType, nullableBoolean)
      ,StructField("IMT_CD", StringType, nullableBoolean)
      ,StructField("ORIG_SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_CD", StringType, nullableBoolean)
      ,StructField("SUBMARKET_NAME", StringType , nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = CLTTest.joinCountryRollUpGeoWithParticipatingMarketTogetRegisteredSubMarket()

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  //2.joinCSETwithRegisteredSubMarkettoGetValidCSET
  test("Test to get joinCSETwithRegisteredSubMarkettoGetValidCSET function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val CLTTest = new CLT()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(
      StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("CUST_SET_NAME", StringType, nullableBoolean)
      ,StructField("CUSTOMER_SET_ID", LongType, nullableBoolean)
      ,StructField("IOT_ID", LongType, nullableBoolean)
      ,StructField("IMT_NAME", StringType, nullableBoolean)
      ,StructField("IMT_CD", StringType, nullableBoolean)
      ,StructField("ORIG_SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_CD", StringType , nullableBoolean)
      ,StructField("SUBMARKET_NAME", StringType , nullableBoolean)
    ))

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = CLTTest.joinCSETwithRegisteredSubMarkettoGetValidCSET()

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  //3.joinValidCSETwithCovereftoGetValidCoveref
  test("Test to get joinValidCSETwithCovereftoGetValidCoveref function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val CLTTest = new CLT()
    val nullableBoolean=true
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)
    val expectedSchema = StructType(Seq(
      StructField("CUSTOMER_SET_ID", LongType, nullableBoolean)
      ,StructField("ITT_COVREF_ID", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("CUST_SET_NAME", StringType, nullableBoolean)
      ,StructField("IOT_ID", LongType, nullableBoolean)
      ,StructField("IMT_NAME", StringType, nullableBoolean)
      ,StructField("IMT_CD", StringType, nullableBoolean)
      ,StructField("ORIG_SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_CD", StringType , nullableBoolean)
      ,StructField("SUBMARKET_NAME", StringType , nullableBoolean)
    ))

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = CLTTest.joinValidCSETwithCovereftoGetValidCoveref(EFFC_DATE_SQL)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  //  4.JoinTargetAccountWithValidCoveref
  test("Test to get JoinTargetAccountWithValidCoveref function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val CLTTest = new CLT()
    val nullableBoolean = true;
    val expectedSchema = StructType(Seq(
      StructField("ITT_COVREF_ID", LongType, nullableBoolean)
      ,StructField("TARGET_ACCOUNT_ID",LongType, nullableBoolean)
      ,StructField("TARGET_ACCT_CD",StringType, nullableBoolean)
      ,StructField("SUBMARKET_ID",LongType, nullableBoolean)
      ,StructField("CUSTOMER_SET_ID",LongType, nullableBoolean)
      ,StructField("IMT_ID",LongType, nullableBoolean)
      ,StructField("CUST_SET_NAME",StringType, nullableBoolean)
      ,StructField("IOT_ID",LongType, nullableBoolean)
      ,StructField("IMT_NAME",StringType, nullableBoolean)
      ,StructField("IMT_CD",StringType, nullableBoolean)
      ,StructField("ORIG_SUBMARKET_ID",LongType, nullableBoolean)
      ,StructField("SUBMARKET_CD",StringType, nullableBoolean)
      ,StructField("SUBMARKET_NAME",StringType, nullableBoolean)
    ))
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark, expectedSchema)
    val actualDataFrame =  CLTTest.joinTargetAccountWithValidCoveref( EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame, actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  //5.joinValidTargetAccountIDwithALT
  test("Test to get joinValidTargetAccountIDwithALT function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val CLTTest = new CLT()
    val nullableBoolean = true;
    val expectedSchema = StructType(Seq(
       StructField("TARGET_ACCOUNT_ID",LongType, nullableBoolean)
      ,StructField("IMT_ID",LongType, nullableBoolean)
      ,StructField("ITT_COVREF_ID",LongType, nullableBoolean)
      ,StructField("TARGET_ACCT_CD",StringType, nullableBoolean)
      ,StructField("SUBMARKET_ID",LongType, nullableBoolean)
      ,StructField("CUSTOMER_SET_ID",LongType, nullableBoolean)
      ,StructField("CUST_SET_NAME",StringType, nullableBoolean)
      ,StructField("IOT_ID",LongType, nullableBoolean)
      ,StructField("IMT_NAME",StringType, nullableBoolean)
      ,StructField("IMT_CD",StringType, nullableBoolean)
      ,StructField("ORIG_SUBMARKET_ID",LongType, nullableBoolean)
      ,StructField("SUBMARKET_CD",StringType, nullableBoolean)
      ,StructField("SUBMARKET_NAME",StringType, nullableBoolean)
      ,StructField("ACCOUNT_LVL_TARGET_ID",LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID",LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT",LongType, nullableBoolean)
      ,StructField("TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("GP_TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_GP_TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("TARGET_SKEW_PCT",DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("GP_TARGET_SKEW_PCT",DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("REASON_CD",StringType, nullableBoolean)
      ,StructField("EFFC_DATE",DateType, nullableBoolean)
      ,StructField("DLET_DATE",DateType, nullableBoolean)
      ,StructField("LAST_ACT_SYS_CD",StringType, nullableBoolean)
      ,StructField("GT10_TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("LT10_TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_CLOUD_TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
    ))
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark, expectedSchema)
    val actualDataFrame =  CLTTest.joinValidTargetAccountIDwithALT(EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame, actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  // 6.joinValidAccountLevelTargetwithModelingVersion
  test("Test to get joinValidAccountLevelTargetwithModelingVersion function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val clt = new CLT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("ACCOUNT_LVL_TARGET_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("CUST_SET_NAME", StringType, nullableBoolean)
      ,StructField("TARGET_ACCT_CD", StringType, nullableBoolean)
      ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("GP_TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("REASON_CD", StringType, nullableBoolean)
      ,StructField("EFFC_DATE", DateType, nullableBoolean)
      ,StructField("DLET_DATE", DateType, nullableBoolean)
      ,StructField("GT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("LT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_CLOUD_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)

    val actualDataFrame = clt.joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  //7.joinCustwithTargetAccountCMRMapping
  test("Test to get joinCustwithTargetAccountCMRMapping function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val CLTTest = new CLT()
    val nullableBoolean = true;
    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID",LongType, nullableBoolean)
      ,StructField("CUST_ACCT_PCT",DoubleType, nullableBoolean)
      ,StructField("ITT_CUST_ID",LongType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark, expectedSchema)
    val actualDataFrame =  CLTTest.joinCustwithTargetAccountCMRMapping(0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame, actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  // 8.getCustWithNoCustPCT
  test("Test to get getCustWithNoCustPCT function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val clt = new CLT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)
    val df1=clt.joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL,0)
    val actualDataFrame = clt.getCustWithNoCustPCT(EFFC_DATE_SQL,0,df1)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  // 9.joinCustWithNoCustPCTwithCalculatedCustPCT
  test("Test to get joinCustWithNoCustPCTwithCalculatedCustPCT function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val clt = new CLT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("ITT_CUST_ID", LongType, nullableBoolean)
      ,StructField("CUST_ACCT_PCT", DoubleType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)
    val df1=clt.joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL,0)
    val actualDataFrame = clt.joinCustWithNoCustPCTwithCalculatedCustPCT(EFFC_DATE_SQL,0,df1)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  // 10.unionCustPCTwithNoCustPCT
  test("Test to get unionCustPCTwithNoCustPCT function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val clt = new CLT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("ITT_CUST_ID", LongType, nullableBoolean)
      ,StructField("CUST_ACCT_PCT", DoubleType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)
    val df1=clt.joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL,0)
    val actualDataFrame = clt.unionCustPCTwithNoCustPCT(EFFC_DATE_SQL,0,df1)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  // 11.joinTargetAccountWithCUSTPCT
  test("Test to get joinTargetAccountWithCUSTPCT function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val clt = new CLT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("ACCOUNT_LVL_TARGET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("CUST_SET_NAME", StringType, nullableBoolean)
      ,StructField("TARGET_ACCT_CD", StringType, nullableBoolean)
      ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("GP_TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("REASON_CD", StringType, nullableBoolean)
      ,StructField("EFFC_DATE", DateType, nullableBoolean)
      ,StructField("DLET_DATE", DateType, nullableBoolean)
      ,StructField("GT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("LT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_CLOUD_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ITT_CUST_ID", LongType, nullableBoolean)
      ,StructField("CUST_ACCT_PCT", DoubleType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)

    val actualDataFrame = clt.joinTargetAccountWithCUSTPCT(EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  // 12.joinTargetAccountCustPCTwithValidCust
  test("Test to get joinTargetAccountCustPCTwithValidCust function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val clt = new CLT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("ITT_CUST_ID", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("ACCOUNT_LVL_TARGET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("CUST_SET_NAME", StringType, nullableBoolean)
      ,StructField("TARGET_ACCT_CD", StringType, nullableBoolean)
      ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("GP_TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("REASON_CD", StringType, nullableBoolean)
      ,StructField("EFFC_DATE", DateType, nullableBoolean)
      ,StructField("DLET_DATE", DateType, nullableBoolean)
      ,StructField("GT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("LT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_CLOUD_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("CUST_ACCT_PCT", DoubleType, nullableBoolean)
      ,StructField("SAP_CUST_NBR", StringType, nullableBoolean)
      ,StructField("CI_CUST_NBR", StringType, nullableBoolean)
      ,StructField("CUSTNUM", StringType, nullableBoolean)
      ,StructField("CTRYNUM", StringType, nullableBoolean)
      ,StructField("CUST_NAME", StringType, nullableBoolean)
      ,StructField("COV_TYPE", StringType, nullableBoolean)
      ,StructField("COV_ID", StringType, nullableBoolean)
      ,StructField("COV_NAME", StringType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)

    val actualDataFrame = clt.joinTargetAccountCustPCTwithValidCust(EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
  // 13.joinTargetCustwithBMDIVPCT
  test("Test to get joinTargetCustwithBMDIVPCT function is returning expected result"){
    val testHelperFunctions = new TestHelperFunctions()
    val clt = new CLT()
    val nullableBoolean=true
    val notnullableBoolean=false
    val expectedSchema = StructType(Seq(
      StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("ITT_CUST_ID", LongType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("ACCOUNT_LVL_TARGET_ID", LongType, nullableBoolean)
      ,StructField("SUBMARKET_ID", LongType, nullableBoolean)
      ,StructField("TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("CUST_SET_NAME", StringType, nullableBoolean)
      ,StructField("TARGET_ACCT_CD", StringType, nullableBoolean)
      ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("GP_TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("REASON_CD", StringType, nullableBoolean)
      ,StructField("EFFC_DATE", DateType, nullableBoolean)
      ,StructField("DLET_DATE", DateType, nullableBoolean)
      ,StructField("GT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("LT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_CLOUD_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("CUST_ACCT_PCT", DoubleType, nullableBoolean)
      ,StructField("SAP_CUST_NBR", StringType, nullableBoolean)
      ,StructField("CI_CUST_NBR", StringType, nullableBoolean)
      ,StructField("CUSTNUM", StringType, nullableBoolean)
      ,StructField("CTRYNUM", StringType, nullableBoolean)
      ,StructField("CUST_NAME", StringType, nullableBoolean)
      ,StructField("COV_TYPE", StringType, nullableBoolean)
      ,StructField("COV_ID", StringType, nullableBoolean)
      ,StructField("COV_NAME", StringType, nullableBoolean)
      ,StructField("BMDIV_ID", LongType, nullableBoolean)
      ,StructField("MODELING_LVL_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val EFFC_DATE_STRING = "2020-01-01"
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE_STRING)

    val actualDataFrame = clt.joinTargetCustwithBMDIVPCT(EFFC_DATE_SQL,0)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
