package com.ibm.fms.bim.dbread

import java.sql.Date

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame

class CustPCT {
  
         val readTable = new ReadTable

  
  
  def getCustPCT(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame ={
    val STRING_FMST_R_ITT_CUST_PCT = "FMST_R_ITT_CUST_PCT"
    val DF_FMST_R_ITT_CUST_PCT = readTable.readTableToDF(STRING_FMST_R_ITT_CUST_PCT)
    DF_FMST_R_ITT_CUST_PCT
      .where(DF_FMST_R_ITT_CUST_PCT("EFFC_DATE")===EFFC_DATE_SQL)
      .where(DF_FMST_R_ITT_CUST_PCT("IMT_ID")===REQ_IMT_ID)
      .select("TARGET_ACCOUNT_ID","BRAND_SUB_SUBGROUP_ID", "ITT_ATTRIB_VALUE_ID_TGT" , "IMT_ID", "ITT_CUST_ID", "CUST_ACCT_PCT")
  }

}