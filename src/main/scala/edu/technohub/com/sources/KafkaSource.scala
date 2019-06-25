package edu.technohub.com.sources

import edu.technohub.com.utils.ConfigManager.KafkaConfig
import edu.technohub.com.utils.Constants

class KafkaSource extends Source {

  val brokers = KafkaConfig.getValue("broker")

  //properties for consuming data from Kafka
  val sourceTopic = KafkaConfig.getValue("source.topic")
  val groupId = KafkaConfig.getValue("source.group.id")
  val fromBeginning = if (KafkaConfig.getBoolean("source.earliest")) "earliest" else "latest"
  val maxOffsetsPerTrigger = KafkaConfig.getValue("source.max.offsets.per.trigger")

  //properties for inserting data into Kafka
  val sinkTopic = KafkaConfig.getValue("sink.topic")

  /**
    * Properties for Spark-Db connection
    */
  override def sinkOptions: Map[String, String] = Map(
    ("kafka.bootstrap.servers", brokers),
    ("topic", sinkTopic)
  )

  /**
    * Properties for Spark-Source source connection
    */
  override def sourceOptions: Map[String, String] = Map(
    ("kafka.bootstrap.servers", brokers),
    ("group.id", groupId),
    ("startingOffsets", fromBeginning),
//    ("maxOffsetsPerTrigger", maxOffsetsPerTrigger), //TODO:check::Enable this when using trigger to control data input in each trigger
    ("subscribe", sourceTopic)
  )

  /** Data source name */
  override def dataSource: String = Constants.KAFKA_SOURCE
}
