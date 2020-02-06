package com.ibm.fms.bim

import com.ibm.fms.bim.dbread.{ConfigCycle}
import com.ibm.fms.bim.Calculate.CalculateCLT

object CLT {

  def  main(args: Array[String]): Unit = {
    val configCycle = new ConfigCycle
    val calculate = new CalculateCLT
    val EFFC_DATE_SQL = configCycle.getEffcDate("ITT_CURRENT_CYCLE")
    val REQ_IMT_ID = 230
    calculate.calculateCLTREV(EFFC_DATE_SQL,REQ_IMT_ID).printSchema()
    
  }
}
