package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame
class ModellingVersion {
  
  
       val readTable = new ReadTable

  def getModellingVersion(REQ_IMT_ID: Int): DataFrame = {

    val STRING_FMST_O_ITT_MODELING_VERSION = "FMST_O_ITT_MODELING_VERSION"

    val DF_FMST_O_ITT_MODELING_VERSION = readTable.readTableToDF(STRING_FMST_O_ITT_MODELING_VERSION)

    DF_FMST_O_ITT_MODELING_VERSION.where("MODELING_VERSION_STATUS = 'REEX'")
      .where(DF_FMST_O_ITT_MODELING_VERSION("IMT_ID")===REQ_IMT_ID)
      .where("REQUEST_TYPE = 'C'")
      .select("BRAND_SUB_SUBGROUP_ID","SUBMARKET_ID","IMT_ID")

  }

}