package edu.technohub.com.utils

object Constants {

  val APP_NAME = "kafka-spark-vertica"
  val SPARK_MASTER = "local[*]"
  val CHECKPOINT_PATH = "src/main/resources/checkpoints"
  val TRIGGERING_TIME = 2L
  val VALUE = "value"
  val ERROR_LOG_LEVEL = "ERROR"

  val TEN_SECONDS = "10 seconds"

  val BROKER = "localhost:9092"
  val TOPIC = "sample-topic"
  val GROUP_ID = "kafka-group"
  val EARLIEST = true
  val TRIGGER_OFFSET = "10"
  val KAFKA_SOURCE = "kafka"
  val VERTICA_SOURCE = "com.vertica.spark.datasource.DefaultSource"

  val ID = "id"
  val NAME = "name"
  val AGE = "age"

  val PERSON_ID = "personId"
  val KCAL_BURNED = "kCalBurned"
  val ACTIVITY_DURATION = "activityDuration"
  val TIMESTAMP = "timestamp"

}
