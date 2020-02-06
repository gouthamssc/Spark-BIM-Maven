package com.ibm.fms.bim.Joins

import java.sql.Date

import com.ibm.fms.bim.dbread._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when}

class CLT {
  def joinCountryRollUpGeoWithParticipatingMarketTogetRegisteredSubMarket(): DataFrame = {

    val countryRollUpGeo = new CountryRollUpGeo
    val participatingMarket = new ParticipatingMarket
    val attributeValue = new AttributeValue

    var dfRegisteredSubmarket = participatingMarket.getParticipatingMarket()
      .where("APPR_STATUS <> 'Canceled'")
      .join(countryRollUpGeo.getCountryRollUpGeo(),Seq("IMT_ID"))
      .select("IOT_ID","IMT_ID","IMT_NAME","IMT_CD","SUBMARKET_ID","SUBMARKET_CD","SUBMARKET_NAME","ITT_ATTRIB_VALUE_ID","BUDGET_SUBMARKET_ID")

    dfRegisteredSubmarket = dfRegisteredSubmarket.withColumn("ORIG_SUBMARKET_ID",dfRegisteredSubmarket("SUBMARKET_ID"))

    dfRegisteredSubmarket=dfRegisteredSubmarket.join(attributeValue.getAttributeValueToDF(),"ITT_ATTRIB_VALUE_ID")

    dfRegisteredSubmarket= dfRegisteredSubmarket.withColumn("SUBMARKET_ID_NEW",when(dfRegisteredSubmarket("ATTRIB_VALUE_CD")==="MKT",lit(0)).otherwise(dfRegisteredSubmarket("BUDGET_SUBMARKET_ID")))
      .withColumn("SUBMARKET_CD_NEW",when(dfRegisteredSubmarket("ATTRIB_VALUE_CD")==="MKT",lit("All")).otherwise(dfRegisteredSubmarket("SUBMARKET_CD")))
      .withColumn("SUBMARKET_NAME_NEW",when(dfRegisteredSubmarket("ATTRIB_VALUE_CD")==="MKT",lit("All")).otherwise(dfRegisteredSubmarket("SUBMARKET_NAME"))).distinct()

    dfRegisteredSubmarket
      .select("IOT_ID","IMT_ID","IMT_NAME","IMT_CD","ORIG_SUBMARKET_ID","SUBMARKET_ID_NEW","SUBMARKET_CD_NEW","SUBMARKET_NAME_NEW")
      .withColumnRenamed("SUBMARKET_ID_NEW","SUBMARKET_ID")
      .withColumnRenamed("SUBMARKET_CD_NEW","SUBMARKET_CD")
      .withColumnRenamed("SUBMARKET_NAME_NEW","SUBMARKET_NAME")
  }

  def joinCSETwithRegisteredSubMarkettoGetValidCSET(): DataFrame ={
    val cSET = new CSET
    val DF_CSET = cSET.getCSET()
    val DF_REGISTERED_SUB_MARKET = joinCountryRollUpGeoWithParticipatingMarketTogetRegisteredSubMarket()

    val DF_VALID_CSET = DF_CSET.join(DF_REGISTERED_SUB_MARKET,Seq("IMT_ID","SUBMARKET_ID")).drop("SUBMARKET_ID")

    DF_VALID_CSET

  }

  def joinValidCSETwithCovereftoGetValidCoveref(EFFC_DATE_SQL: Date): DataFrame = {
    val coveref= new Covref
    val DF_VALID_CSET = joinCSETwithRegisteredSubMarkettoGetValidCSET()
    val DF_COVREF = coveref.getCovref(EFFC_DATE_SQL: Date)

    val DF_VALID_COVREF = DF_COVREF.join(DF_VALID_CSET,Seq("CUSTOMER_SET_ID"))
      .drop("EFFC_DATE","DLET_DATE")

    DF_VALID_COVREF
  }
  
  
  def joinTargetAccountWithValidCoveref(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame = {
    val targetAcct =  new TargetAcct
    val DF_VALID_COVEREF = joinValidCSETwithCovereftoGetValidCoveref(EFFC_DATE_SQL)
    val DF_TARGET_ACCOUNT = targetAcct.getTargetAccountID(EFFC_DATE_SQL, REQ_IMT_ID)

    DF_TARGET_ACCOUNT.join(DF_VALID_COVEREF,"ITT_COVREF_ID").where(DF_TARGET_ACCOUNT("SUBMARKET_ID")===DF_VALID_COVEREF("ORIG_SUBMARKET_ID"))
   }

  
  def joinValidTargetAccountIDwithALT(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame = {
    
        val accountLvlTarget = new AccountLvlTarget

    val DF_ALT = accountLvlTarget.getAccountLevelTarget(REQ_IMT_ID)
    
    val DF_VALID_TARGET_ACCOUNT_ID = joinTargetAccountWithValidCoveref(EFFC_DATE_SQL,REQ_IMT_ID)
    DF_VALID_TARGET_ACCOUNT_ID.join(DF_ALT,Seq("TARGET_ACCOUNT_ID","IMT_ID"))
  }
  
     
  
  def joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame = {
    
            val modelingVersion = new ModellingVersion

    val DF_ALT_WITH_VALID_TARGET_ACCOUNT = joinValidTargetAccountIDwithALT(EFFC_DATE_SQL,REQ_IMT_ID)
    val DF_MODELLING_VERSION = modelingVersion.getModellingVersion(REQ_IMT_ID)

    DF_ALT_WITH_VALID_TARGET_ACCOUNT.join(DF_MODELLING_VERSION,Seq("SUBMARKET_ID","IMT_ID","BRAND_SUB_SUBGROUP_ID"))
      .select("TARGET_ACCOUNT_ID","ACCOUNT_LVL_TARGET_ID","BRAND_SUB_SUBGROUP_ID","IMT_ID","SUBMARKET_ID","ITT_ATTRIB_VALUE_ID_TGT","TARGET_AMOUNT","GP_TARGET_AMOUNT","ADJ_TARGET_AMOUNT","ADJ_GP_TARGET_AMOUNT","CUST_SET_NAME","TARGET_ACCT_CD","TARGET_SKEW_PCT","GP_TARGET_SKEW_PCT","REASON_CD","EFFC_DATE","DLET_DATE","GT10_TARGET_AMOUNT","LT10_TARGET_AMOUNT","ADJ_CLOUD_TARGET_AMOUNT")
  }

  def joinCustwithTargetAccountCMRMapping( REQ_IMT_ID: Int): DataFrame = {

    val targetAccountCMRMapping = new TargetAccountCMRMapping
    val cust = new Cust


    val DF_VALID_CUST = cust.getValidCust(REQ_IMT_ID).select("ITT_CUST_ID")
    val DF_VALID_ITT_CUST = targetAccountCMRMapping.getValidCMRMapping(REQ_IMT_ID)
    var df= DF_VALID_CUST.join(DF_VALID_ITT_CUST,Seq("ITT_CUST_ID"))

    df= df.groupBy("TARGET_ACCOUNT_ID").count().orderBy("TARGET_ACCOUNT_ID").withColumnRenamed("count","CNT_CUSTOMER")
    df =  df.withColumn("CUST_ACCT_PCT",when(df("CNT_CUSTOMER")<=>0 , lit(1.0)/df("CNT_CUSTOMER")))
      .drop("CNT_CUSTOMER")
    val cte_CM=df.join(targetAccountCMRMapping.getValidCMRMapping(REQ_IMT_ID),Seq("TARGET_ACCOUNT_ID"))

    cte_CM
  }

  def getCustWithNoCustPCT(EFFC_DATE_SQL: Date,REQ_IMT_ID: Int ,DF_VALID_ACCOUNT_LEVEL_TARGET: DataFrame): DataFrame = {

    val custPCT = new CustPCT

    val vld_cust = custPCT.getCustPCT(EFFC_DATE_SQL, REQ_IMT_ID).select("TARGET_ACCOUNT_ID", "BRAND_SUB_SUBGROUP_ID", "ITT_ATTRIB_VALUE_ID_TGT", "IMT_ID")

    val var1 = DF_VALID_ACCOUNT_LEVEL_TARGET.select("TARGET_ACCOUNT_ID", "BRAND_SUB_SUBGROUP_ID", "ITT_ATTRIB_VALUE_ID_TGT", "IMT_ID")

    val DF_NO_PCT = var1.except(vld_cust)


    DF_NO_PCT

  }

  def joinCustWithNoCustPCTwithCalculatedCustPCT(EFFC_DATE_SQL: Date,REQ_IMT_ID: Int , DF_VALID_ACCOUNT_LEVEL_TARGET: DataFrame): DataFrame = {

    val DF_CALCULATED_CUST_PCT = joinCustwithTargetAccountCMRMapping(REQ_IMT_ID)
    val DF_CUST_WITH_NO_CUST_PCT = getCustWithNoCustPCT(EFFC_DATE_SQL , REQ_IMT_ID , DF_VALID_ACCOUNT_LEVEL_TARGET)

    DF_CALCULATED_CUST_PCT.join(DF_CUST_WITH_NO_CUST_PCT,Seq("TARGET_ACCOUNT_ID"))
      .select("TARGET_ACCOUNT_ID","BRAND_SUB_SUBGROUP_ID", "ITT_ATTRIB_VALUE_ID_TGT" , "IMT_ID", "ITT_CUST_ID", "CUST_ACCT_PCT")
  }

  def unionCustPCTwithNoCustPCT(EFFC_DATE_SQL: Date,REQ_IMT_ID: Int ,DF_VALID_ACCOUNT_LEVEL_TARGET: DataFrame): DataFrame = {

    val custPCT = new CustPCT

    val DF_CUST_WITH_NO_PCT = joinCustWithNoCustPCTwithCalculatedCustPCT(EFFC_DATE_SQL,REQ_IMT_ID,DF_VALID_ACCOUNT_LEVEL_TARGET)

    val DF_CUST_PCT = custPCT.getCustPCT(EFFC_DATE_SQL,REQ_IMT_ID)

    DF_CUST_PCT.union(DF_CUST_WITH_NO_PCT)

  }
  
  
   def joinTargetAccountWithCUSTPCT(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame ={

    val DF_VALID_ACCOUNT_LEVEL_TARGET = joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL,REQ_IMT_ID)
    val DF_CUST_PCT = unionCustPCTwithNoCustPCT(EFFC_DATE_SQL,REQ_IMT_ID,DF_VALID_ACCOUNT_LEVEL_TARGET)

    DF_VALID_ACCOUNT_LEVEL_TARGET.join(DF_CUST_PCT,Seq("TARGET_ACCOUNT_ID","BRAND_SUB_SUBGROUP_ID","ITT_ATTRIB_VALUE_ID_TGT","IMT_ID"))
  }
  
  
   
   def joinTargetAccountCustPCTwithValidCust(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame = {
     
    val cust = new Cust
    val DF_TARGET_ACCOUNT_CUST_PCT = joinTargetAccountWithCUSTPCT(EFFC_DATE_SQL,REQ_IMT_ID)
    val DF_VALID_CUST = cust.getValidCust(REQ_IMT_ID)
    DF_TARGET_ACCOUNT_CUST_PCT.join(DF_VALID_CUST,Seq("ITT_CUST_ID","IMT_ID"))
  }
   
   
    def joinTargetCustwithBMDIVPCT(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame = {
      
    val bMDivPCT = new BMDivPCT
    val DF_TARGET_ACCOUNT_CUST = joinTargetAccountCustPCTwithValidCust(EFFC_DATE_SQL,REQ_IMT_ID)
    val DF_BMDIV_PCT = bMDivPCT.getBMDivPCT()
    DF_TARGET_ACCOUNT_CUST.join(DF_BMDIV_PCT,Seq("BRAND_SUB_SUBGROUP_ID","ITT_ATTRIB_VALUE_ID_TGT"))

  }

}