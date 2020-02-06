package com.ibm.fms.bim.dbread

import java.sql.Date

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame

class TargetAcct {
  
     val readTable = new ReadTable

  
  def getTargetAccountID(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame ={


    val STRING_FMST_O_ITT_TARGET_ACCT = "FMST_O_ITT_TARGET_ACCT"

    val DF_FMST_O_ITT_TARGET_ACCT = readTable.readTableToDF(STRING_FMST_O_ITT_TARGET_ACCT)

    DF_FMST_O_ITT_TARGET_ACCT
      .where("APPR_STATUS = 'Approved'")
      .where(DF_FMST_O_ITT_TARGET_ACCT("IMT_ID")===REQ_IMT_ID)
      .select("TARGET_ACCOUNT_ID","ITT_COVREF_ID","TARGET_ACCT_CD","SUBMARKET_ID","EFFC_DATE")
      .where(DF_FMST_O_ITT_TARGET_ACCT("EFFC_DATE")===EFFC_DATE_SQL)
      .select("TARGET_ACCOUNT_ID","ITT_COVREF_ID","TARGET_ACCT_CD","SUBMARKET_ID")
  }
     
     

}