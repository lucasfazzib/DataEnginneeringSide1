// Databricks notebook source
import org.apache.spark.sql.functions._

//Bronze config
val bronzeDb = "db_zap_project_bronze"
val bronzeTb = "tb_zap_imoveis"
val bronzeId = s"$bronzeDb.$bronzeTb"


//Silver config
val silverDb = "db_zap_project_silver"
val silverTb = "tb_zap_imoveis"
val silverId = s"$silverDb.$silverTb"

val bronzeDF = spark.read.table(bronzeId)

// COMMAND ----------

val preSilverDF = spark.read.json(bronzeDF.select($"value").as[String])
//display(preSilverDF)

val silverDF =preSilverDF.select(
                          expr("*"),
                          $"address.*", 
                          $"address.geoLocation.location.*",
                          $"pricingInfos.*")
                   .drop(
                     "address", "images", "pricingInfos", "geoLocation"
              )

// COMMAND ----------

display(bronzeDF2)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS db_zap_project_silver

// COMMAND ----------

if(!spark.catalog.tableExists(silverId)) {
  silverDF.write
           .format("delta")
           .mode("append")
           .option("path", "adl://adlsbigdatadatabricks.azuredatalakestore.net/silver/")
           .saveAsTable(silverId)
} else {
  silverDF.createOrReplaceTempView("vw_source")
  spark.sql("""
    MERGE INTO ${silverId} as target
    USING vw_source as source
    ON target.id = source.id
    WHEN MATCHED AND source.updatedAt > target.updatedAt THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
  """)
}

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) from db_zap_project_silver.tb_zap_imoveis

// COMMAND ----------

display(spark.sql("select * from " + silverId))

// COMMAND ----------


