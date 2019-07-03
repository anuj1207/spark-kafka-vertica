package edu.technohub.com.app.batch

import edu.technohub.com.models.Person
import edu.technohub.com.services.SparkService
import edu.technohub.com.sources.{KafkaSource, VerticaSource}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This application reads data to Kafka and saves it to Vertica in Batch mode
  * Can add some logic here accordingly
  */
object KafkaToVerticaApplication extends SparkService(new KafkaSource, new VerticaSource) with App{

  val spark: SparkSession = createSparkSession()

  val df: DataFrame = inbound.readFromSource(spark, inbound.sourceOptions)

  val parsedDataFrame = df.transform(parseFromOneColumn(_, Person.schema))

  val transformedDF: DataFrame = parsedDataFrame.transform(transformationLogic) //TODO::check::Add you logic inside `transformationLogic`

  outbound.saveToSource(transformedDF)

  spark.stop()

}
