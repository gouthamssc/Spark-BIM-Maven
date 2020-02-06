package com.ibm.fms.bim.dbread

import java.sql.Date

import com.ibm.fms.bim.dbconnections.{DatabaseConnection, ReadTable}
import org.apache.spark.sql.DataFrame

class ConfigCycle {
  
    val readTable = new ReadTable
    val databaseRead = new DatabaseConnection
    val getattributeValue = new AttributeValue

    def getCycleDate(CycleCD: String,CastEffcDateAs: String = "EFFC_DATE", CastDletDateAs: String ="DLET_DATE" ): DataFrame ={

    val STRING_CONFIG_CYCLE = "CONFIG_CYCLE"
    val DF_CONFIG_CYCLE = readTable.readTableToDF(STRING_CONFIG_CYCLE)
    val DF_FMST_R_ITT_ATTRIB_VALUE_CURRENT_CYCLE = getattributeValue.getAttributeValueToDF("ATTRIB_VALUE_DESC","ATTRIB_VALUE_CD",""+CycleCD+"","int")
    val DF_CYCLEDATE = DF_CONFIG_CYCLE.join(DF_FMST_R_ITT_ATTRIB_VALUE_CURRENT_CYCLE,DF_CONFIG_CYCLE("CYCLE_ID")===DF_FMST_R_ITT_ATTRIB_VALUE_CURRENT_CYCLE("ATTRIB_VALUE_DESC"))
      .select("EFFC_DATE","DLET_DATE")
      .withColumnRenamed("EFFC_DATE",CastEffcDateAs)
      .withColumnRenamed("DLET_DATE", CastDletDateAs)
    DF_CYCLEDATE
  }
    def getEffcDate(CycleCD: String): Date = {
    val DF_CYCLE_DATE = getCycleDate(CycleCD)
    val ROW_CYCLE_DATE = DF_CYCLE_DATE.first()
    val EFFC_DATE = ROW_CYCLE_DATE(0).toString()
    val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE)
    EFFC_DATE_SQL
  }
}