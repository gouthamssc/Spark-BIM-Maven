package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame

class AttributeValue{

def getAttributeValueToDF(selectColumnName: String,conditionColumnName: String, conditionColumnValue: String , castType: String = null):DataFrame = {

    val DF_FMST_R_ITT_ATTRIB_VALUE = getAttributeValueToDF()

    if(castType==null){
        DF_FMST_R_ITT_ATTRIB_VALUE
        .select(DF_FMST_R_ITT_ATTRIB_VALUE(selectColumnName))
        .where(""+conditionColumnName+" = '"+conditionColumnValue+"'")
    }
    else {
          DF_FMST_R_ITT_ATTRIB_VALUE
          .select(DF_FMST_R_ITT_ATTRIB_VALUE(selectColumnName).cast(castType))
          .where(""+conditionColumnName+" = '"+conditionColumnValue+"'")
    }
}

def getAttributeValueToDF():DataFrame = {

    val readTable = new ReadTable
    val STRING_FMST_R_ITT_ATTRIB_VALUE = "FMST_R_ITT_ATTRIB_VALUE"
    val DF_FMST_R_ITT_ATTRIB_VALUE = readTable.readTableToDF(STRING_FMST_R_ITT_ATTRIB_VALUE)
DF_FMST_R_ITT_ATTRIB_VALUE

}
}