package com.ibm.fms.bim.dbread

import java.sql.Date

import com.ibm.fms.bim.Joins.CLT
import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame
class Covref {

   val readTable = new ReadTable
   val clt = new CLT

  def getCovref(EFFC_DATE_SQL: Date): DataFrame = {

    val STRING_FMST_O_ITT_COV_PLAN_CONFIG = "FMST_O_ITT_COV_PLAN_CONFIG"

    val DF_FMST_O_ITT_COV_PLAN_CONFIG = readTable.readTableToDF(STRING_FMST_O_ITT_COV_PLAN_CONFIG)
    val DF_VALIDCSET = clt.joinCSETwithRegisteredSubMarkettoGetValidCSET()

   DF_FMST_O_ITT_COV_PLAN_CONFIG
      .select("CUSTOMER_SET_ID","ITT_COVREF_ID","EFFC_DATE","APPR_STATUS")
      .where("APPR_STATUS = 'Approved'")
      .where(DF_FMST_O_ITT_COV_PLAN_CONFIG("EFFC_DATE")===EFFC_DATE_SQL)
      .drop("APPR_STATUS")
  }
}