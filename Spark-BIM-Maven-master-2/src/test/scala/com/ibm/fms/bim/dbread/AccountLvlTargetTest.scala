package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.dbread.AccountLvlTarget
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class AccountLvlTargetTest extends FunSuite with SparkSessionTestWrapper{
    test("Column count of get Account Level target function is returning as expected"){
      val testHelperFunctions = new TestHelperFunctions()
      val accountLvlTarget = new AccountLvlTarget()
      val nullableBoolean=true
      val expectedSchema = StructType(Seq(
         StructField("ACCOUNT_LVL_TARGET_ID",LongType, nullableBoolean)
        ,StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
        ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
        ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
        ,StructField("TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
        ,StructField("GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
        ,StructField("ADJ_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
        ,StructField("ADJ_GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
        ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
        ,StructField("GP_TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
        ,StructField("REASON_CD", StringType, nullableBoolean)
        ,StructField("EFFC_DATE", DateType, nullableBoolean)
        ,StructField("DLET_DATE", DateType, nullableBoolean)
        ,StructField("LAST_ACT_SYS_CD", StringType, nullableBoolean)
        ,StructField("IMT_ID", LongType, nullableBoolean)
        ,StructField("GT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
        ,StructField("LT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
        ,StructField("ADJ_CLOUD_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ))
      val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
      val actualDataFrame = accountLvlTarget.getAccountLevelTarget(0)

      assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    }
  test("Column name of get Account Level target function is returning as expected"){
    val testHelperFunctions = new TestHelperFunctions()
    val accountLvlTarget = new AccountLvlTarget()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(
      StructField("ACCOUNT_LVL_TARGET_ID",LongType, nullableBoolean)
      ,StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("GP_TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("REASON_CD", StringType, nullableBoolean)
      ,StructField("EFFC_DATE", DateType, nullableBoolean)
      ,StructField("DLET_DATE", DateType, nullableBoolean)
      ,StructField("LAST_ACT_SYS_CD", StringType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("GT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("LT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_CLOUD_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = accountLvlTarget.getAccountLevelTarget(0)

    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
  }
  test("Column Schema of get Account Level target function is returning as expected"){
    val testHelperFunctions = new TestHelperFunctions()
    val accountLvlTarget = new AccountLvlTarget()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(
      StructField("ACCOUNT_LVL_TARGET_ID",LongType, nullableBoolean)
      ,StructField("TARGET_ACCOUNT_ID", LongType, nullableBoolean)
      ,StructField("BRAND_SUB_SUBGROUP_ID", LongType, nullableBoolean)
      ,StructField("ITT_ATTRIB_VALUE_ID_TGT", LongType, nullableBoolean)
      ,StructField("TARGET_AMOUNT",DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_GP_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("GP_TARGET_SKEW_PCT", DataTypes.createDecimalType(11,10), nullableBoolean)
      ,StructField("REASON_CD", StringType, nullableBoolean)
      ,StructField("EFFC_DATE", DateType, nullableBoolean)
      ,StructField("DLET_DATE", DateType, nullableBoolean)
      ,StructField("LAST_ACT_SYS_CD", StringType, nullableBoolean)
      ,StructField("IMT_ID", LongType, nullableBoolean)
      ,StructField("GT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("LT10_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
      ,StructField("ADJ_CLOUD_TARGET_AMOUNT", DataTypes.createDecimalType(31,10), nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = accountLvlTarget.getAccountLevelTarget(0)

    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
