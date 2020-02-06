package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame

class CSET {

  val readTable = new ReadTable
  
  def getCSET(): DataFrame ={

    val STRING_FMST_O_ITT_CUSTOMER_SET = "FMST_O_ITT_CUSTOMER_SET"

    val DF_FMST_O_ITT_CUSTOMER_SET = readTable.readTableToDF(STRING_FMST_O_ITT_CUSTOMER_SET)

    DF_FMST_O_ITT_CUSTOMER_SET.select("CUST_SET_NAME","IMT_ID","SUBMARKET_ID","CUSTOMER_SET_ID")

  }
}