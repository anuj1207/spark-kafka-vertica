package edu.technohub.com.sources

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.SaveMode.Append

/**
  * Base trait for Source types which can be used for adding new data sources like Elasticsearch and Cassandra in existing application
  * */
trait Source {

  /**
    * Properties for Spark-Source sink connection
    */
  def sinkOptions: Map[String, String]

  /**
    * Properties for Spark-Source source connection
    */
  def sourceOptions: Map[String, String]

  /**Data source name*/
  def dataSource: String

  /**
    * Read data as dataFrame from source, using class from Source spark connector jar and above properties
    *
    * @param sparkSession object of SparkSession which basically represents the master in spark SQL
    * @param opts         properties required by spark to read data from source
    */
  def readFromSource(sparkSession: SparkSession, opts: Map[String, String] = sourceOptions, format: String = dataSource): DataFrame = {
    sparkSession.read.format(format).options(opts).load()
  }

  /**
    * Save dataFrame to source, using class from Source spark connector jar and above properties
    *
    * @param dataFrame flattened dataFrame with metadata and sensor values
    * @param opts      properties required by spark to save data to source
    * @param mode      By default Append mode: when saving a DataFrame to a data source, if data/table already exists,
    *                  contents of the DataFrame are expected to be appended to existing data.
    */
  def saveToSource(dataFrame: DataFrame, opts: Map[String, String] = sinkOptions, format: String = dataSource, mode: SaveMode = Append): Unit = {
    dataFrame.write.format(format).options(opts).mode(mode).save()
  }

}
