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
    * @param sparkSession The object of SparkSession or the master
    * @param opts         properties required by spark to read data from source
    * @param format       The format of the source from where data is read as DataFrame
    * @return             DataFrame referencing to the actual data
    */
  def readFromSource(sparkSession: SparkSession, opts: Map[String, String] = sourceOptions, format: String = dataSource): DataFrame = {
    sparkSession.read.format(format).options(opts).load()
  }

  /**
    * Save dataFrame to source, using class from Source spark connector jar and above properties
    *
    * @param dataFrame The DataFrame which needs to be saved
    * @param opts      properties required by spark to save data to source
    * @param format    The format of the data source where the data needs to be saved
    * @param mode      the expected behavior of saving a DataFrame to a data source (append / overwrite etc)
    */
  def saveToSource(dataFrame: DataFrame, opts: Map[String, String] = sinkOptions, format: String = dataSource, mode: SaveMode = Append): Unit = {
    dataFrame.write.format(format).options(opts).mode(mode).save()
  }

}
