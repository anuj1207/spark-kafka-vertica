package edu.technohub.com.services

import edu.technohub.com.sources.Source
import edu.technohub.com.utils.ConfigManager.SparkConfig
import edu.technohub.com.utils.Constants._
import edu.technohub.com.utils.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration._

class SparkService(inbound: Source, outbound: Source) extends Logging {

  val master: String = SparkConfig.getValueOpt("master").getOrElse{noConfigFound("master");SPARK_MASTER}
  val appName: String = SparkConfig.getValueOpt("app.name").getOrElse{noConfigFound("app.name");APP_NAME}
  val logLevel: String = SparkConfig.getValueOpt("log.level").getOrElse{noConfigFound("log.level");ERROR_LOG_LEVEL}
  val checkpointLocation: String = SparkConfig.getValueOpt("checkpoint.location").getOrElse{noConfigFound("checkpoint.location");CHECKPOINT_PATH}
  val triggeringTime: Duration = SparkConfig.getLongOpt("trigger.time").getOrElse{noConfigFound("trigger.time");TRIGGERING_TIME}.seconds

  def createSparkSession(config: Map[String, String] = Map.empty): SparkSession = {

    val spark = SparkSession.builder()
      .config(new SparkConf().setAll(config))
      .appName(appName)
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  def createStreamingDataFrame(sparkSession: SparkSession, opts: Map[String, String] = inbound.sourceOptions, format: String = inbound.dataSource): DataFrame =
    sparkSession.readStream.format(format).options(opts).load()

  /**To parse DataFrame into a specified schema*/
  def parseDataFrame(dataFrame: DataFrame, schema: StructType): DataFrame =
    dataFrame.selectExpr(s"CAST($VALUE AS STRING)")
      .select(from_json(col(VALUE), schema).as(VALUE))
      .select(col(s"$VALUE.*"))

  /**Use your transform logic here for your DataFrame*/
  val transformationLogic: DataFrame => DataFrame = { dataFrame =>
    //TODO:check::Add your logic here
    dataFrame
  }

  def writeStreamingDF(dataFrame: DataFrame): StreamingQuery =
    dataFrame.writeStream.outputMode(OutputMode.Append)
      .option("checkpointLocation", checkpointLocation)
//      .trigger(Trigger.ProcessingTime(triggeringTime)) //TODO:check::Enable this to enable triggering after a certain time
      .foreachBatch((ds, _) => outbound.saveToSource(ds)).start()

}
