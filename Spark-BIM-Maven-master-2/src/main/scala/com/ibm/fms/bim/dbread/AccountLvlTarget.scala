package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame
class AccountLvlTarget {
  
  val readTable = new ReadTable

  def getAccountLevelTarget(REQ_IMT_ID: Int): DataFrame = {

    val STRING_FMSV1_O_ITT_ACCOUNT_LVL_TARGET = "FMSV1_O_ITT_ACCOUNT_LVL_TARGET"

    val DF_FMSV1_O_ITT_ACCOUNT_LVL_TARGET = readTable.readTableToDF(STRING_FMSV1_O_ITT_ACCOUNT_LVL_TARGET)

    DF_FMSV1_O_ITT_ACCOUNT_LVL_TARGET
      .where(DF_FMSV1_O_ITT_ACCOUNT_LVL_TARGET("IMT_ID")===REQ_IMT_ID)
      .where("APPR_STATUS='Approved'")
      .where("TARGET_AMOUNT <> 0 OR GP_TARGET_AMOUNT <> 0 OR ADJ_TARGET_AMOUNT <> 0 OR ADJ_GP_TARGET_AMOUNT <> 0 OR GT10_TARGET_AMOUNT <> 0 OR LT10_TARGET_AMOUNT <> 0 OR ADJ_CLOUD_TARGET_AMOUNT <> 0")
      .drop("APPR_STATUS","LAST_ACT_USER_ID","CREATE_TIMESTAMP","LAST_UPT_TIME","SUBMARKET_ID")
  }
}