package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.{SparkSessionTestWrapper, TestHelperFunctions}
import com.ibm.fms.bim.dbread.AttributeValue
import org.apache.spark.sql.types.{ DataTypes, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class AttributeValueTest extends FunSuite with SparkSessionTestWrapper{
  test("ITT_ATTRIB_VALUE_ID - Test to get Attribute value function is returning expected result without cast"){
    val testHelperFunctions = new TestHelperFunctions()
    val attribValue = new AttributeValue()
    val nullableBoolean=true
    val expectedSchema = StructType(Seq(StructField("ITT_ATTRIB_VALUE_ID", LongType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = attribValue.getAttributeValueToDF("ITT_ATTRIB_VALUE_ID","ATTRIB_VALUE_CD","AMRE")

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }

  test("ATTRIB_VALUE_DESC - Test to get Attribute value function is returning expected result with cast"){
    val testHelperFunctions = new TestHelperFunctions()
    val attribValue = new AttributeValue()
    val castParameter = "int"
    val nullableBoolean=true

    val expectedSchema = StructType(Seq(StructField("ATTRIB_VALUE_DESC", IntegerType, nullableBoolean)
    ))
    val expectedDataFrame = testHelperFunctions.createEmptyDataFrameWithGivenSchema(spark,expectedSchema)
    val actualDataFrame = attribValue.getAttributeValueToDF("ATTRIB_VALUE_DESC","ATTRIB_VALUE_CD","AMRE",castParameter)

    assert(testHelperFunctions.assertDataFrameColumnCountEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameColumnNameEquals(expectedDataFrame,actualDataFrame))
    assert(testHelperFunctions.assertDataFrameSchemaEquals(expectedDataFrame,actualDataFrame))
  }
}
