package edu.technohub.com.sources

import edu.technohub.com.utils.ConfigManager.VerticaConfig
import edu.technohub.com.utils.Constants

class VerticaSource extends Source with Serializable {

  /**
    * Properties for Spark-Vertica connection
    */
  override val sinkOptions: Map[String, String] = Map(
    "db" -> VerticaConfig.getValue("db"), // Database name
    "user" -> VerticaConfig.getValue("user"), // Database username
    "password" -> VerticaConfig.getValue("password"), // Password
    "table" -> VerticaConfig.getValue("sink.table"), // vertica table name
    "dbschema" -> VerticaConfig.getValue("sink.dbschema"), // schema of vertica where the table will be residing
    "host" -> VerticaConfig.getValue("host"), // Host on which vertica is currently running
    "hdfs_url" -> VerticaConfig.getValue("hdfs_url"), // HDFS directory url in which intermediate orc file will persist before sending it to vertica
    "web_hdfs_url" -> VerticaConfig.getValue("web_hdfs_url") // FileSystem interface for HDFS over the Web
  )

  /**
    * Properties for Spark-Vertica source connection
    */
  override val sourceOptions: Map[String, String] = Map(
    "db" -> VerticaConfig.getValue("db"), // Database name
    "user" -> VerticaConfig.getValue("user"), // Database username
    "password" -> VerticaConfig.getValue("password"), // Password
    "table" -> VerticaConfig.getValue("source.table"), // vertica table name
    "dbschema" -> VerticaConfig.getValue("source.dbschema"), // schema of vertica where the table will be residing
    "host" -> VerticaConfig.getValue("host"), // Host on which vertica is currently running
    "numPartitions" -> VerticaConfig.getValue("source.numPartitions") // Num of partitions to be created in resulting DataFrame
  )


  /** Data source name */
  override def dataSource: String = Constants.VERTICA_SOURCE
}
