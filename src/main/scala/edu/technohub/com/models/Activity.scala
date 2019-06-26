package edu.technohub.com.models

import java.sql.Timestamp

import org.apache.spark.sql.types._
import edu.technohub.com.utils.Constants.{PERSON_ID, KCAL_BURNED, ACTIVITY_DURATION, TIMESTAMP}

case class Activity(personId: Int, kCalBurned: Double, activityDuration:Long, timestamp: Timestamp)

object Activity{

  //TODO::check::/*Create schema for your model*/
  def schema:StructType = StructType(
    List(
      StructField(PERSON_ID, IntegerType, nullable = true),
      StructField(KCAL_BURNED, DoubleType, nullable = true),
      StructField(ACTIVITY_DURATION, LongType, nullable = true),
      StructField(TIMESTAMP, TimestampType , nullable = true)
    )
  )
}