package com.ibm.fms.bim.dbconnections

import java.util.Properties
import java.util.ResourceBundle

class DatabaseConnection {
  var resource = ResourceBundle.getBundle("lp01")
  def getConnectionProperties(): Properties ={
    val devProperties = new Properties()
    devProperties.put("user", getDBUserName())
    devProperties.put("password", getDBPassword())
    devProperties.put("sslConnection", "true")
    devProperties.put("sslTrustStoreLocation", getDBJKSFile())
    devProperties
  }

  def getDBUserName(): String = {
    val username = resource.getString("BIM.DB2" + ".user")
    username
  }

  
  def getDBPassword(): String = {
    val password = resource.getString("BIM.DB2" + ".password")
    password
  }

  def getDBJKSFile(): String = {

    val jksfilelocation =resource.getString("BIM.DB2" + ".jkspath")
//    val jksfilelocation = "C:\\Users\\SairamanKumar\\Desktop\\DB connection\\ibm-gold-truststore.jks"
    jksfilelocation
  }

  def getDBport(): String = {
    val port = resource.getString("BIM.DB2" + ".port")
    port
  }

  def getDBip(): String = {
    val ip = resource.getString("BIM.DB2" + ".host")
    ip
  }

  def getDBName(): String = {
    val name = resource.getString("BIM.DB2" + ".database")
    name
  }

  def getDBUrl(): String = {


//     val url = "jdbc:db2://" + getDBip() + ":" + getDBport() + "/" +getDBName()
    val url =   "jdbc:db2://cap-sg-prd-4.securegateway.appdomain.cloud:19881/LP01CDA1"

    url

    //val url =   "jdbc:db2://cap-sg-prd-4.securegateway.appdomain.cloud:19881/LP01CDA1"

  }

  def getDBSchema(): String = {
    val schema = resource.getString("BIM.DB2" + ".schema")
    schema
  }

  
}
