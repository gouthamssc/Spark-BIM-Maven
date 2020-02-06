package com.ibm.fms.bim.dbread

import com.ibm.fms.bim.dbconnections.{DatabaseConnection, ReadTable}
import org.apache.spark.sql.DataFrame

class RequestTracking {
  
       val readTable = new ReadTable
      val databaseRead = new DatabaseConnection
      val getattributeValue = new AttributeValue
  
  def getRequestTrackingID(RequestTypeID: String,RequestStatusID: String,ApprovedStatus: String): DataFrame ={

    val STRING_FMST_O_ITT_REQUEST_TRACKING = "FMST_O_ITT_REQUEST_TRACKING"
    val DF_FMST_O_ITT_REQUEST_TRACKING = readTable.readTableToDF(STRING_FMST_O_ITT_REQUEST_TRACKING)
    val DF_FMST_R_ITT_ATTRIB_VALUE_REQUEST_TYPE_ID = getattributeValue.getAttributeValueToDF("ITT_ATTRIB_VALUE_ID","ATTRIB_VALUE_CD",RequestTypeID)
    val DF_FMST_R_ITT_ATTRIB_VALUE_REQUEST_STATUS_ID =  getattributeValue.getAttributeValueToDF("ITT_ATTRIB_VALUE_ID","ATTRIB_VALUE_CD",RequestStatusID)

    
    
    
    val DF_IMT_ID_REQUEST_TRACKING = DF_FMST_O_ITT_REQUEST_TRACKING.where("APPR_STATUS ='"+ApprovedStatus+"'")
      .select("IMT_ID","REQ_TYPE_ID","REQ_STATUS_ID").distinct()
      .join(DF_FMST_R_ITT_ATTRIB_VALUE_REQUEST_TYPE_ID, DF_FMST_R_ITT_ATTRIB_VALUE_REQUEST_TYPE_ID("ITT_ATTRIB_VALUE_ID")===DF_FMST_O_ITT_REQUEST_TRACKING("REQ_TYPE_ID"))
      .select("IMT_ID","REQ_STATUS_ID")
      .join(DF_FMST_R_ITT_ATTRIB_VALUE_REQUEST_STATUS_ID, DF_FMST_R_ITT_ATTRIB_VALUE_REQUEST_STATUS_ID("ITT_ATTRIB_VALUE_ID")===DF_FMST_O_ITT_REQUEST_TRACKING("REQ_STATUS_ID")).select("IMT_ID")

    DF_IMT_ID_REQUEST_TRACKING
  }
}