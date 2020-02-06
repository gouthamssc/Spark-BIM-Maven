package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
class BMDivPCT {
  
                        val readTable = new ReadTable

   def getBMDivPCT(): DataFrame ={
     

    val STRING_FMST_R_ITT_BMDIV_PCT = "FMST_R_ITT_BMDIV_PCT"
    val DF_FMST_R_ITT_BMDIV_PCT=readTable.readTableToDF(STRING_FMST_R_ITT_BMDIV_PCT)

    DF_FMST_R_ITT_BMDIV_PCT.select("BMDIV_ID","MODELING_LVL_PCT","BRAND_SUB_SUBGROUP_ID","ITT_ATTRIB_VALUE_ID_TGT").withColumn("Mark",lit(1))
  }
  
  
}