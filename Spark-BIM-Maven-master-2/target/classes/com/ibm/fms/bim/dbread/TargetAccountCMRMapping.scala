package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame



class TargetAccountCMRMapping {
  
  val readTable = new ReadTable

  
  def getValidCMRMapping(REQ_IMT_ID: Int):DataFrame={
    
    
      val STRING_FMST_O_ITT_TARGET_ACCT_CMR_MAPPING = "FMST_O_ITT_TARGET_ACCT_CMR_MAPPING"
     val DF_FMST_O_ITT_TARGET_ACCT_CMR_MAPPING = readTable.readTableToDF(STRING_FMST_O_ITT_TARGET_ACCT_CMR_MAPPING).select("TARGET_ACCOUNT_ID","ITT_CUST_ID")

    DF_FMST_O_ITT_TARGET_ACCT_CMR_MAPPING
  }
  
  
  
}