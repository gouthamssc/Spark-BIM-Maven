package com.ibm.fms.bim

import com.ibm.fms.bim.dbread.AttributeValue
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class UtilityTest extends FunSuite with SparkSessionTestWrapper {
  test("Column count of get Attrib value function is returning as expected without cast parameter passed"){
    val testHelperFunctions = new TestHelperFunctions()
    val attribValue = new AttributeValue()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(StructField("ITT_ATTRIB_VALUE_ID", StringType, nullableBoolean)
//      ,StructField("lastName", IntegerType, true),StructField("middleName", IntegerType, true)
      )
    )
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = attribValue.getAttributeValueToDF("ITT_ATTRIB_VALUE_ID","ATTRIB_VALUE_CD","AMRE")

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
  }
  test("Column count of get Attrib value function is returning as expected with cast parameter passed"){
    val testHelperFunctions = new TestHelperFunctions()
    val attribValue = new AttributeValue()
    val castParameter = "int"
    val nullableBoolean=true

    val expectedSchema = StructType(Seq(StructField("ITT_ATTRIB_VALUE_ID", IntegerType, nullableBoolean)
      //      ,StructField("lastName", IntegerType, true),StructField("middleName", IntegerType, true)
    )
    )
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = attribValue.getAttributeValueToDF("ITT_ATTRIB_VALUE_ID","ATTRIB_VALUE_CD","AMRE",castParameter)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
  }
}