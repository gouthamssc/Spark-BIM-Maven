package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.ReadTable
import org.apache.spark.sql.DataFrame
class CountryRollUpGeo {
  
               val readTable = new ReadTable

  def getCountryRollUpGeo(): DataFrame = {

    


    val STRING_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO = "FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO"

    val DF_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO = readTable.readTableToDF(STRING_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO)

    DF_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO
  }
}