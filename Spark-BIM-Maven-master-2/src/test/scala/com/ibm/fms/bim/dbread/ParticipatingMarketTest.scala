package com.ibm.fms.bim.dbread
import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite
class ParticipatingMarketTest extends FunSuite with SparkSessionTestWrapper {
  test("check whether the  getParticipatingMarket returns the expected Schema") {
    val testHelperFunctions = new TestHelperFunctions()
    val participatingMarket = new ParticipatingMarket()
    val nullableBoolean = true;

    val expectedSchema = StructType(Seq(StructField("PARTICIPATING_MARKET_ID", LongType, nullableBoolean)
      , StructField("IMT_ID", LongType, nullableBoolean)
      , StructField("ITT_ATTRIB_VALUE_ID", LongType, nullableBoolean)
      , StructField("EFFC_DATE", DateType, nullableBoolean)
      , StructField("DLET_DATE", DateType, nullableBoolean)
      , StructField("APPR_STATUS", StringType, nullableBoolean)
      , StructField("LAST_ACT_USER_ID", LongType, nullableBoolean)
      , StructField("CREATE_TIMESTAMP", TimestampType, nullableBoolean)
      , StructField("LAST_UPT_TIME", TimestampType, nullableBoolean)
      , StructField("LAST_ACT_SYS_CD", StringType, nullableBoolean)
      , StructField("WWCONSOL_IMT_CD", StringType, nullableBoolean)
      , StructField("PROCESSING_GROUP", StringType, nullableBoolean)
      , StructField("ITT_ATTRIB_VALUE_ID_SWSIGN", LongType, nullableBoolean)
      , StructField("ITT_ATTRIB_VALUE_ID_SWREV", LongType, nullableBoolean)
      , StructField("GT10_INDC", LongType, nullableBoolean)
      , StructField("ITT_ATTRIB_VALUE_ID_LATE_BUDGT", LongType, nullableBoolean)
    )
    )

    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = participatingMarket.getParticipatingMarket()

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}