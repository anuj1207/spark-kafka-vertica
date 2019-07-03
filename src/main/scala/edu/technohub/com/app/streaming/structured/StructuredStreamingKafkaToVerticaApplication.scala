package edu.technohub.com.app.streaming.structured

import edu.technohub.com.models.Activity
import edu.technohub.com.services.SparkService
import edu.technohub.com.sources.{KafkaSource, VerticaSource}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This application receives data from Kafka and saves it to Vertica in Streaming mode
  * through Structured Streaming
  * Can plug in watermarking / deduplication in this application
  * Can also add some logic accordingly
  */
object StructuredStreamingKafkaToVerticaApplication extends SparkService(new KafkaSource, new VerticaSource) with App{

  val spark: SparkSession = createSparkSession()

  val df: DataFrame = createStreamingDataFrame(spark)

  val parsedDataFrame: DataFrame = df.transform(parseFromOneColumn(_, Activity.schema)) //TODO::check::Use the schema of your model here

  val transformedDF: DataFrame = parsedDataFrame
//    .transform(watermarkingOperation(_)) //TODO::check::Enable this to start watermarking to drop late data
//    .transform(deduplicateOperation(_)) //TODO::check::Enable this to start deduplication
    .transform(transformationLogic) //TODO::check::Add you logic inside `transformationLogic`

  customWriteStreamingDF(transformedDF).awaitTermination()

}
