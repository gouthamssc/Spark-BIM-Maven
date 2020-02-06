package com.ibm.fms.bim.Calculate

import java.sql.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
import com.ibm.fms.bim.Joins.CLT

class CalculateCLT {
  def calculateCLTREV(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame = {

    val cLT = new CLT

    val DF_CLT_REV = cLT.joinTargetCustwithBMDIVPCT(EFFC_DATE_SQL,REQ_IMT_ID)

    DF_CLT_REV
      .withColumn("TARGET_AMOUNT",
        when(DF_CLT_REV("TARGET_AMOUNT").isNull || DF_CLT_REV("CUST_ACCT_PCT").isNull || DF_CLT_REV("MODELING_LVL_PCT").isNull , 0)
          .otherwise(DF_CLT_REV("TARGET_AMOUNT")*DF_CLT_REV("CUST_ACCT_PCT")*DF_CLT_REV("MODELING_LVL_PCT")))
      .withColumn("GP_TARGET_AMOUNT",
        when(DF_CLT_REV("GP_TARGET_AMOUNT").isNull || DF_CLT_REV("CUST_ACCT_PCT").isNull || DF_CLT_REV("MODELING_LVL_PCT").isNull , 0)
          .otherwise(DF_CLT_REV("GP_TARGET_AMOUNT")*DF_CLT_REV("CUST_ACCT_PCT")*DF_CLT_REV("MODELING_LVL_PCT")))
      .withColumn("ADJ_TARGET_AMOUNT",
        when(DF_CLT_REV("ADJ_TARGET_AMOUNT").isNull || DF_CLT_REV("CUST_ACCT_PCT").isNull || DF_CLT_REV("MODELING_LVL_PCT").isNull , 0)
          .otherwise(DF_CLT_REV("ADJ_TARGET_AMOUNT")*DF_CLT_REV("CUST_ACCT_PCT")*DF_CLT_REV("MODELING_LVL_PCT")))
      .withColumn("ADJ_GP_TARGET_AMOUNT",
        when(DF_CLT_REV("ADJ_GP_TARGET_AMOUNT").isNull || DF_CLT_REV("CUST_ACCT_PCT").isNull || DF_CLT_REV("MODELING_LVL_PCT").isNull , 0)
          .otherwise(DF_CLT_REV("ADJ_GP_TARGET_AMOUNT")*DF_CLT_REV("CUST_ACCT_PCT")*DF_CLT_REV("MODELING_LVL_PCT")))
      .withColumn("GT10_TARGET_AMOUNT",
        when(DF_CLT_REV("GT10_TARGET_AMOUNT").isNull || DF_CLT_REV("CUST_ACCT_PCT").isNull || DF_CLT_REV("MODELING_LVL_PCT").isNull , 0)
          .otherwise(DF_CLT_REV("GT10_TARGET_AMOUNT")*DF_CLT_REV("CUST_ACCT_PCT")*DF_CLT_REV("MODELING_LVL_PCT")))
      .withColumn("LT10_TARGET_AMOUNT",
        when(DF_CLT_REV("LT10_TARGET_AMOUNT").isNull || DF_CLT_REV("CUST_ACCT_PCT").isNull || DF_CLT_REV("MODELING_LVL_PCT").isNull , 0)
          .otherwise(DF_CLT_REV("LT10_TARGET_AMOUNT")*DF_CLT_REV("CUST_ACCT_PCT")*DF_CLT_REV("MODELING_LVL_PCT")))
      .withColumn("ADJ_CLOUD_TARGET_AMOUNT",
        when(DF_CLT_REV("ADJ_CLOUD_TARGET_AMOUNT").isNull || DF_CLT_REV("CUST_ACCT_PCT").isNull || DF_CLT_REV("MODELING_LVL_PCT").isNull , 0)
          .otherwise(DF_CLT_REV("ADJ_CLOUD_TARGET_AMOUNT")*DF_CLT_REV("CUST_ACCT_PCT")*DF_CLT_REV("MODELING_LVL_PCT")))
      .withColumn("APPR_STATUS",lit("Approved"))
      .withColumn("LAST_ACT_SYS_CD",lit("FMS#LCLT"))
      .withColumn("LAST_ACT_USER_ID",lit(0))
      .withColumn("CREATE_TIMESTAMP",lit(current_timestamp()))
      .withColumn("LAST_UPT_TIME",lit(current_timestamp()))
      .drop("CUST_PCT_ID","CUST_ACCT_PCT","MODELING_LVL_PCT")
  }
}
