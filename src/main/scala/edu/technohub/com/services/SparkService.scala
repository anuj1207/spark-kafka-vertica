package edu.technohub.com.services

import edu.technohub.com.sources.Source
import edu.technohub.com.utils.ConfigManager.SparkConfig
import edu.technohub.com.utils.Constants._
import edu.technohub.com.utils.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.concurrent.duration._

class SparkService(val inbound: Source, val outbound: Source) extends Logging {

  val master: String = SparkConfig.getValueOpt("master").getOrElse{noConfigFound("master");SPARK_MASTER}
  val appName: String = SparkConfig.getValueOpt("app.name").getOrElse{noConfigFound("app.name");APP_NAME}
  val logLevel: String = SparkConfig.getValueOpt("log.level").getOrElse{noConfigFound("log.level");ERROR_LOG_LEVEL}
  val checkpointLocation: String = SparkConfig.getValueOpt("checkpoint.location").getOrElse{noConfigFound("checkpoint.location");CHECKPOINT_PATH}
  val triggeringTime: Duration = SparkConfig.getLongOpt("trigger.time").getOrElse{noConfigFound("trigger.time");TRIGGERING_TIME}.seconds

  /**
    * Creates an object of SparkSession with given config
    * @param config The configuration with which SparkSession needs to be created
    * */
  def createSparkSession(config: Map[String, String] = Map.empty): SparkSession = {

    val spark = SparkSession.builder()
      .config(new SparkConf().setAll(config))
      .appName(appName)
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  /**
    * Creates a streaming DataFrame with given data source with the help of SparkSession
    * @param sparkSession The object of SparkSession or the master
    * @param opts         Properties to create a connection with the source to read data
    * @param format       The format of source with which data is to be read
    * @return             DataFrame with reference to the data
    */
  def createStreamingDataFrame(sparkSession: SparkSession, opts: Map[String, String] = inbound.sourceOptions, format: String = inbound.dataSource): DataFrame =
    sparkSession.readStream.format(format).options(opts).load()

  /**
    * This method is to write streaming data to those sinks which aren't yet supported by Structured Streaming (like Vertica)
    * @param dataFrame  A streaming DataFrame is required otherwise this method will not work
    * @return           A streaming query which will continue to run for each trigger/new data comes from input source till the time of termination
    */
  def customWriteStreamingDF(dataFrame: DataFrame): StreamingQuery =
    dataFrame.writeStream.outputMode(OutputMode.Append)
      .option("checkpointLocation", checkpointLocation)
      //      .trigger(Trigger.ProcessingTime(triggeringTime)) //TODO:check::Enable this to enable triggering after a certain time
      .foreachBatch((ds, _) => outbound.saveToSource(ds)).start()

  /**
    * This method is to write streaming data to those sinks which are supported by Structured Streaming (like Kafka)
    * @param dataFrame  A streaming DataFrame is required otherwise this method will not work
    * @param opts       Properties to create a connection with the source to write data
    * @param format     The format of source where data is to be written
    * @param mode       Mode of saving data (complete / append / update)
    * @return           A streaming query which will continue to run for each trigger/new data comes from input source till the time of termination
    */
  def writeStreamingDF(dataFrame: DataFrame, opts: Map[String, String] = outbound.sourceOptions, format: String = outbound.dataSource, mode: OutputMode = Append): StreamingQuery =
    dataFrame.writeStream.format(format)
      .options(opts)
      //      .trigger(Trigger.ProcessingTime(triggeringTime)) //TODO:check::Enable this to enable triggering after a certain time
      .outputMode(mode).start()

  /**
    * Applies watermarking on late data and drop the late data exceeding threshold
    * @param dataFrame
    * @param timeColumn name of the column which describes the event time
    * @param threshold  Threshold time after which all the data is rejected {ex: "10 seconds" or "10 minutes"}
    * @return
    */
  def watermarkingOperation(dataFrame: DataFrame, timeColumn: String = TIMESTAMP, threshold: String = TEN_SECONDS): DataFrame =
    dataFrame.withWatermark(timeColumn, threshold)

  /**
    * Remove duplicate data over given columns
    * @param dataFrame
    * @param columns    The columns which needs to be considered for removing duplicates
    * @return
    */
  def deduplicateOperation(dataFrame: DataFrame, columns: List[String] = List(PERSON_ID, TIMESTAMP)): Dataset[Row] =
    dataFrame.dropDuplicates(columns)

  /**
    * To parse data of one column of specified DataFrame into a given schema
    * @param dataFrame  DataFrame containing the data
    * @param schema     The schema which will be the schema of output DataFrame
    * @param columnName The column which needs to be parsed using the specified schema
    * @return           The parsed DataFrame which will have the same columns as schema
    */
  def parseFromOneColumn(dataFrame: DataFrame, schema: StructType, columnName: String = VALUE): DataFrame =
    dataFrame.select(col(columnName).cast(StringType))
      .select(from_json(col(columnName), schema).as(columnName))
      .select(col(columnName + ".*"))

  /**
    * Merging all values as JSON into one column value
    * @param dataFrame        DataFrame to be worked upon
    * @param columnNames      The columns which needs to be merged as JSON
    * @param finalColumnName  The output column
    * @return                 DataFrame which only has the finalColumn having all the data from given column merged as JSON
    */
  def mergeIntoOneColumn(dataFrame: DataFrame, columnNames: List[String], finalColumnName: String = VALUE): DataFrame = {
    val columns = columnNames.map(col)
    dataFrame.select(to_json(struct(columns: _*)).as(finalColumnName))
  }

  /**
    * Write your transform logic here for your DataFrame
    */
  val transformationLogic: DataFrame => DataFrame = { dataFrame =>
    //TODO:check::Add your logic here
    dataFrame
  }

}
